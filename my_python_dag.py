from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

def greet():
    print("hello airflow")

def add(number1, number2):
    result = number1 + number2
    print(f"{number1} + {number2} = {result}")

def subtract(number1, number2):
    result = number1 - number2
    print(f"{number1} - {number2} = {result}")

def finishing():
    print("finish")

default_args = {
    "owner":"nadir",
    "retries":3,
    "retry_delay":timedelta(minutes=2),
}

with DAG(
    dag_id="my_python_dag_v1",
    default_args=default_args,
    description="DAG with Python Operator",
    start_date=datetime(2024, 4, 4, 3),
    schedule_interval="@daily",
) as dag:
    task1 = PythonOperator(
        task_id="task_1",
        python_callable=greet,
    )

    task2 = PythonOperator(
        task_id="task_2",
        python_callable=add,
        op_kwargs={"number1":4,"number2":5},
    )

    task3 = PythonOperator(
        task_id="task_3",
        python_callable=subtract,
        op_kwargs={"number1":10, "number2":8},
    )

    task4 = PythonOperator(
        task_id="task_4",
        python_callable=finishing,
    )

    # task1 -> task2 -> task4
    #   |                 |
    #   ----> task3 ------|

    # first approach
    # task1 >> task2
    # task1 >> task3

    # recommended
    task1 >> [task2, task3]

    task2 >> task4
    task3 >> task4