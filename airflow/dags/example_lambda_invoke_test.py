# invoke lambda function dag
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from datetime import datetime

logger = logging.getLogger(__name__)


def log_lambda_result(**kwargs):
    """
    LambdaInvokeFunctionOperator의 XCom 응답을 로깅하는 함수
    """
    task_instance = kwargs["ti"]
    response = task_instance.xcom_pull(task_ids="lambda_invoke_task")
    logger.info(f"Lambda Response: {response}")
    if response:
        try:
            response_payload = json.loads(response)
            logger.info(f"Lambda Response: {response_payload}")
        except json.JSONDecodeError:
            logger.error(f"Failed to parse Lambda response: {response}")
    else:
        logger.warning("No response received from Lambda function.")


with DAG(
    "example.lambda_invoke_test",
    schedule_interval=None,
    tags=["test"],
    description="Test DAG: Invoke Lambda function and log the response.",
) as dag:
    lambda_invoke_task = LambdaInvokeFunctionOperator(
        task_id="lambda_invoke_task",
        function_name="lambda_example",
        aws_conn_id="aws_default",
        payload=json.dumps(
            {"from": "airflow", "timestamp": datetime.now().isoformat()}
        ),
    )

    log_result_task = PythonOperator(
        task_id="log_result_task",
        python_callable=log_lambda_result,
        provide_context=True,
    )

    lambda_invoke_task >> log_result_task
