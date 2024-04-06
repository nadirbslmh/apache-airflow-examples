from datetime import datetime, timedelta

from airflow import DAG

from custom_operators.average_operator import AverageOperator

default_args = {
    "owner":"nadir",
    "retries":3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="custom_ops_dag",
    default_args=default_args,
    description="custom dag",
    start_date=datetime(2024, 4, 3),
    schedule_interval="@daily",
) as dag:
    average_task = AverageOperator(
        task_id="average_task",
        data=[1,2,3,4,5,6,7,8,9],
    )