"""
## Dynamically map over the result of an upstream task

This DAG will list dogs and create a dynamically mapped task instance to print
information about each dog.
"""

from airflow.decorators import dag, task
from pendulum import datetime


@dag(
    schedule="@daily",
    start_date=datetime(2023, 6, 1),
    catchup=False,
    tags=["dynamic_task_mapping", "101_presentation_dag"],
)
def good_dogs():
    @task
    def get_dogs():
        return "Avery", "Piglet", "Peanut", "Butter"

    @task
    def print_dog_info(adjective, dog):
        print(f"{dog} is a {adjective} dog!")

    print_dog_info.partial(adjective="very good").expand(dog=get_dogs())


good_dogs()
