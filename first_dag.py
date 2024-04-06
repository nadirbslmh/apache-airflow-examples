from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner":"nadir",
    "retries":3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="my_first_dag",
    default_args=default_args,
    description="my first dag in airflow",
    start_date=datetime(2024, 1, 4, 2),
    schedule_interval="@daily",
) as dag:
    taskA = BashOperator(
        task_id="task_a",
        bash_command="echo first task",
    )

    taskB = BashOperator(
        task_id="task_b",
        bash_command="echo second task",
    )

    taskC = BashOperator(
        task_id="task_c",
        bash_command="echo i am DONE"
    )

    # taskA -> taskB -> taskC
    taskA >> taskB
    taskB >> taskC
