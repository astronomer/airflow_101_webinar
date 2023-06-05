"""
## Query a list from an API and dynamically map a task to print each item

This DAG will query the list of astronauts currently in space from the Open Notify API and
print each astronaut's name as a separate dynamically mapped task.
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests


@dag(
    schedule="@daily", start_date=datetime(2023, 6, 1), catchup=False, tags=["webinar"]
)
def upstream_astronauts():
    @task(outlets=[Dataset("current_astronauts")])
    def get_astronauts(**context):
        ASTRONAUT_API = f"http://api.open-notify.org/astros.json"
        r = requests.get(ASTRONAUT_API)
        number_of_people_in_space = r.json()["number"]
        list_of_people_in_space = r.json()["people"]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

    @task
    def print_astronaut_craft(greeting, person_in_space):
        craft = person_in_space["craft"]
        name = person_in_space["name"]

        print(f"{name} is currently in space flying on the {craft}! {greeting}")

    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=get_astronauts()
    )


upstream_astronauts()
