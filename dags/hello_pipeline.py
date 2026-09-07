from datetime import datetime, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator


def say_hello():
    print("Hello from Airflow! The pipeline is alive.")

def say_goodbye():
    print("Task complete. Shutting down cleanly.")

with DAG(
    dag_id="hello_pipeline",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,   # only runs when triggered manually, not on a timer
    catchup=False,
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

    goodbye_task = PythonOperator(
        task_id="say_goodbye",
        python_callable=say_goodbye,
    )

    hello_task >> goodbye_task   # this defines the dependency: hello runs before goodbye