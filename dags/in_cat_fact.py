from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator
from pendulum import datetime
import requests


@dag(
    start_date=datetime(2023, 10, 18),
    schedule="@daily",
    catchup=False,
)
def in_cat_fact():
    @task
    def get_cat_fact():
        r = requests.get("https://catfact.ninja/fact")
        return r.json()["fact"]

    get_cat_fact_obj = get_cat_fact()

    print_cat_fact = BashOperator(
        task_id="print_cat_fact",
        bash_command=f"echo {get_cat_fact_obj}",
    )

    chain(get_cat_fact_obj, print_cat_fact)


in_cat_fact()
