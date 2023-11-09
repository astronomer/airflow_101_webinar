from airflow.decorators import dag, task
from pendulum import datetime
import requests
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models.baseoperator import chain

POSTGRES_CONN_ID = "postgres_default"


@dag(start_date=datetime(2023, 10, 18), schedule=None, catchup=False, tags=["etl"])
def etl_example_simple():
    @task
    def extract_cat_fact():
        r = requests.get("https://catfact.ninja/fact").json()
        print(r)
        return r["fact"]

    cat_fact = extract_cat_fact()

    @task
    def transform_cat_fact(cat_fact: str):
        length = len(cat_fact)
        return length

    # the dependency between extract_cat_fact and transform_cat_fact is implicit
    # and created automatically by the task flow API
    cat_fact_length = transform_cat_fact(cat_fact)

    create_cat_fact_table_if_not_exists = PostgresOperator(
        task_id="create_cat_fact_table_if_not_exists",
        postgres_conn_id="postgres_default",
        sql="CREATE TABLE IF NOT EXISTS cat_fact (fact VARCHAR(255), length INT);",
    )

    load_cat_fact = PostgresOperator(
        task_id="load_cat_fact",
        postgres_conn_id="postgres_default",
        sql=f"""INSERT INTO cat_fact (fact, length) 
            VALUES ('{cat_fact}', '{cat_fact_length}');""",
    )

    query_cat_facts = PostgresOperator(
        task_id="query_cat_fact",
        postgres_conn_id="postgres_default",
        sql="SELECT * FROM cat_fact;",
    )

    # dependencies involving traditional operators need to be explicitly defined
    chain(
        [create_cat_fact_table_if_not_exists, cat_fact_length],
        load_cat_fact,
        query_cat_facts,
    )


etl_example_simple()
