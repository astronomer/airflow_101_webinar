"""
### Run sentiment analysis using HuggingFace and the Astro Python SDK

This DAG uses the Astro Python SDK to ingest a joke from the Manatee Joke API 
and HuggingFace to run a sentiment analysis on the joke.
"""

from airflow.decorators import dag
from astro import sql as aql
from astro.sql.table import Table
from airflow.operators.empty import EmptyOperator

import pandas as pd
from pendulum import datetime
import logging
import requests
import os

task_logger = logging.getLogger("airflow.task")

DB_CONN_ID = "postgres_default"
HUGGINGFACE_API_TOKEN = os.getenv("HUGGINGFACE_API_TOKEN")
SENTIMENT_ANALYSIS_MODEL = "cardiffnlp/twitter-roberta-base-sentiment"


def replace_labels(data):
    label_mapping = {
        "LABEL_2": "positive_sentiment",
        "LABEL_1": "neutral_sentiment",
        "LABEL_0": "negative_sentiment",
    }

    for sublist in data:
        for item in sublist:
            # Replace the label with the corresponding sentiment
            if item["label"] in label_mapping:
                item["label"] = label_mapping[item["label"]]

    return data


@aql.dataframe
def get_sentences_from_api():
    "Get a random joke from the Manatee Joke API."

    r = requests.get("https://manateejokesapi.herokuapp.com/manatees/random")
    df = pd.json_normalize(r.json())
    df.columns = [col_name.upper() for col_name in df.columns]
    df = df.rename(columns={"ID": "JOKE_ID"})
    print(df)
    return df


@aql.transform
def transform(in_table):
    return """
            SELECT "SETUP", "PUNCHLINE"
            FROM {{ in_table }};
            """


@aql.dataframe
def sentiment_analysis(df: pd.DataFrame, huggingface_api_token: str, model_name: str):
    "Run a sentiment analysis on the setup and punchline of the joke."
    headers = {"Authorization": f"Bearer {huggingface_api_token}"}

    query = list(df["SETUP"].values) + list(df["PUNCHLINE"].values)

    api_url = f"https://api-inference.huggingface.co/models/{model_name}"

    response = requests.post(api_url, headers=headers, json={"inputs": query})

    if response.status_code == 200:
        response_text = response.json()
        response_text = replace_labels(response_text)
        task_logger.info(response_text)

    else:
        task_logger.info(
            f"Request failed with status code {response.status_code}: {response.text}"
        )


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["machine_learning"],
)
def ml_example_manatee_sentiment():
    start = EmptyOperator(task_id="start")

    # ------------------------- #
    # Ingest and transform data #
    # ------------------------- #

    in_data = get_sentences_from_api(
        output_table=Table(conn_id=DB_CONN_ID, name="in_joke")
    )

    transformed_data = transform(
        in_table=in_data, output_table=Table(conn_id=DB_CONN_ID, name="joke_table")
    )

    # ---------------------- #
    # Run sentiment analysis #
    # ---------------------- #

    run_model = sentiment_analysis(
        df=transformed_data,
        huggingface_api_token=HUGGINGFACE_API_TOKEN,
        model_name=SENTIMENT_ANALYSIS_MODEL,
    )

    start >> in_data

    transformed_data >> run_model

    aql.cleanup()


ml_example_manatee_sentiment()
