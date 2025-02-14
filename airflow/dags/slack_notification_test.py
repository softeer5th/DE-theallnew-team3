from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime
import random


def random_task():
    if random.random() < 0.5:
        raise Exception("Random failure")
    else:
        print("Success")
        return "Success"


with DAG(
    "example.slack_notification_test",
    schedule_interval=None,
    tags=["test"],
    description="Test DAG: 50% success, 50% failure. Send slack notification on success or failure.",
) as dag:
    random_success_task = PythonOperator(
        task_id="random_success_task",
        python_callable=random_task,
    )
    send_slack_message = SlackWebhookOperator(
        task_id="slack_webhook_send_text",
        slack_webhook_conn_id="slack_default",
        message=("Test Message"),
    )

    random_success_task >> send_slack_message
