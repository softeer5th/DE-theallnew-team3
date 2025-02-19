from airflow import DAG
from airflow.operators.python import PythonOperator
from common.slack import slack_info_message, slack_handle_task_failure
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
        on_failure_callback=slack_handle_task_failure,
    )

    send_slack_message = slack_info_message(
        message="Test Message", dag=dag, task_id="slack_webhook_send_text"
    )

    random_success_task >> send_slack_message
