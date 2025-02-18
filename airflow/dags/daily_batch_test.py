from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_date(ds):
    print("date: ", ds)


with DAG(
    "example.daily_batch_test",
    schedule_interval="0 12 * * *",
    start_date=datetime(2025, 1, 15),
    end_date=datetime(2025, 3, 15),
    tags=["test"],
    catchup=True,
    description="Test DAG: Backfill Test. 오늘부터 2025년 1월 15일까지 매일 실행",
) as dag:
    print_date_task = PythonOperator(
        task_id="print_date_task",
        python_callable=print_date,
    )

    print_date_task
