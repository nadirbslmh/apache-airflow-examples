from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def send_gift_package(ti):
    ti.xcom_push(key="gift", value="secret gift")

def send_package(ti):
    ti.xcom_push(key="package", value="regular package")

def receive_items(ti):
    gift = ti.xcom_pull(task_ids="send_gift", key="gift")
    package = ti.xcom_pull(task_ids="send_package", key="package")

    print(f"received gift: {gift}")
    print(f"received package: {package}")

default_args = {
    "owner":"nadir",
    "retries":3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="send_packages",
    default_args=default_args,
    description="send many packages",
    start_date=datetime(2024, 4, 2, 2),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="send_gift",
        python_callable=send_gift_package,
    )

    task2 = PythonOperator(
        task_id="send_package",
        python_callable=send_package,
    )

    task3 = PythonOperator(
        task_id="receive_items",
        python_callable=receive_items,
    )

    [task1, task2] >> task3
