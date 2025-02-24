from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime


def print_date(ds):
    print("date: ", ds)


with DAG(
    "example.monthly_batch_test",
    schedule_interval="0 0 15 * *",
    start_date=datetime(2024, 1, 15),
    tags=["test"],
    catchup=True,
    description="Test DAG: Backfill Test. 2024년 1월 15일부터 매월 1일 실행",
) as dag:
    print_date_task = PythonOperator(
        task_id="print_date_task",
        python_callable=print_date,
    )

    print_date_task
