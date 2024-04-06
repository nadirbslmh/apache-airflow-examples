from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner":"nadir",
    "retries":5,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="postgres_dag_v2",
    default_args=default_args,
    description="Postgres Operator in Airflow",
    start_date=datetime(2024, 4, 3),
    schedule_interval="@daily",
) as dag:
    create_table_task = PostgresOperator(
        task_id="create_table_task",
        postgres_conn_id="my_postgres",
        sql="sql/create_table.sql",
    )

    insert_records_task = PostgresOperator(
        task_id="insert_records_task",
        postgres_conn_id="my_postgres",
        sql="sql/insert_records.sql",
    )

    create_table_task >> insert_records_task