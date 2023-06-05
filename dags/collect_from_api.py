"""
## Query an API and write the response to MinIO

This DAG will need a MinIO connection named `minio_default` to be configured in Airflow, as 
well as an environment variable named `MY_API` that contains a URL to an API that returns JSON.
"""

from pendulum import datetime
from airflow.decorators import dag, task
from include.minio.minio import LocalFilesystemToMinIOOperator
import requests
import os


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
)
def collect_from_api():
    @task
    def extract():
        r = requests.get(os.environ["MY_API"])
        return r.json()

    LocalFilesystemToMinIOOperator(
        minio_conn_id="minio_default",
        task_id="write_to_minio",
        json_serializeable_information=extract(),
        bucket_name="extract",
        object_name="{{ logical_date }}_api_response.json",
    )


collect_from_api()
