from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "owner": "nadir",
    "retries":3,
    "retry_delay":timedelta(minutes=2),
}

@dag(
    dag_id="simple_taskflow_dag_v1",
    default_args=default_args,
    start_date=datetime(2024, 4, 3),
    schedule_interval="@daily",
)
def delivery_service_etl():
    @task()
    def get_packages():
        return ["gift","package","secret box"]
    
    @task()
    def send_items(items):
        for item in items:
            print(f"sending item: {item}...")
    

    # execute tasks
    items = get_packages()
    send_items(items)

delivery_service_dag = delivery_service_etl()
