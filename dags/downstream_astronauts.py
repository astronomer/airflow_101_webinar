"""
## Print the number of astronauts currently in space

This DAG will pull the number of astronauts currently in space from the 
XCom pushed by the upstream_astronauts DAG.
"""

from airflow import Dataset
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain
from pendulum import datetime


@dag(
    start_date=datetime(2023, 6, 1),
    schedule=[Dataset("current_astronauts")],
    catchup=False,
    tags=["webinar"],
)
def downstream_astronauts():
    print_num_astronauts = BashOperator(
        task_id="print_num_astronauts",
        bash_command="echo $NUM_ASTRONAUTS",
        env={
            "NUM_ASTRONAUTS": " {{ ti.xcom_pull(\
                                    dag_id='upstream_astronauts',\
                                    task_ids='get_astronauts',\
                                    key='number_of_people_in_space',\
                                    include_prior_dates=True\
                                ) }} "
        },
    )

    print_reaction = BashOperator(
        task_id="print_reaction",
        bash_command="echo This is awesome!",
    )

    chain(print_num_astronauts, print_reaction)
    # print_num_astronauts >> print_reaction


downstream_astronauts()
