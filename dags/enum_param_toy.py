"""
## Choose between two labeled tasks based on a param of the enum type

This DAG has two labeled branches, Path A and Path B, the choice between the branches is made
by selecting your path via run DAG w/ config from a dropdown menu.
"""

from airflow.decorators import dag, task_group, task
from airflow.operators.empty import EmptyOperator
from airflow.utils.edgemodifier import Label
from airflow.models.baseoperator import chain
from airflow.models.param import Param
from pendulum import datetime


@dag(
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    params={"branch": Param("path_a", enum=["path_a", "path_b"])},
    tags=["101_presentation_dag", "params", "branching"]
)
def enum_param_toy():
    @task.branch
    def pick_a_path(**context):
        path = context["params"]["branch"]
        if path == "path_a":
            return "extract_source_A"
        if path == "path_b":
            return "extract_source_B"

    @task
    def report():
        print("Yay! :)")

    RedEmptyOperator_A = EmptyOperator(task_id="extract_source_A")
    RedEmptyOperator_B = EmptyOperator(task_id="extract_source_B")
    RedEmptyOperator_A.ui_color = "#FF0000"
    RedEmptyOperator_B.ui_color = "#FF0000"

    # make sure to set trigger_rule="none_failed_or_skipped" on the task running
    # after the branches to avoid it being skipped
    OrangeEmptyOperator = EmptyOperator(
        task_id="transform_1", trigger_rule="none_failed_or_skipped"
    )
    OrangeEmptyOperator.ui_color = "#FF9900"

    YellowEmptyOperator = EmptyOperator(task_id="transform_2")
    YellowEmptyOperator.ui_color = "#F4EA77"

    GreenEmptyOperator = EmptyOperator(task_id="check_data_quality")
    GreenEmptyOperator.ui_color = "#059F2D"

    BlueEmptyOperator = EmptyOperator(task_id="transform_3")
    BlueEmptyOperator.ui_color = "#014FE8"

    VioletEmptyOperator = EmptyOperator(task_id="load")
    VioletEmptyOperator.ui_color = "#9101A1"

    @task_group
    def train_models():
        LightBlueEmptyOperator_1 = EmptyOperator(task_id="model_a")
        LightBlueEmptyOperator_1.ui_color = "#5BCEFA"

        BabyPinkEmptyOperator_1 = EmptyOperator(task_id="model_b")
        BabyPinkEmptyOperator_1.ui_color = "#F5A9B8"

        WhiteEmptyOperator = EmptyOperator(task_id="model_c")
        WhiteEmptyOperator.ui_color = "#FFFFFF"

    pick_path = pick_a_path()

    model_tasks_obj = train_models()

    # set edge labels
    pick_path >> Label("Path B") >> RedEmptyOperator_A,
    pick_path >> Label("Path A") >> RedEmptyOperator_B

    chain(
        [RedEmptyOperator_A, RedEmptyOperator_B],
        OrangeEmptyOperator,
        YellowEmptyOperator,
        GreenEmptyOperator,
        BlueEmptyOperator,
        VioletEmptyOperator,
    )

    GreenEmptyOperator >> model_tasks_obj

    [VioletEmptyOperator, model_tasks_obj] >> report()


enum_param_toy()
