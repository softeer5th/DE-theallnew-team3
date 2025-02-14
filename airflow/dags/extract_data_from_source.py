# invoke lambda function dag
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)

logger = logging.getLogger(__name__)


def success_callback(**kwargs):
    """
    LambdaInvokeFunctionOperator의 XCom 응답을 로깅하는 함수
    """
    task_instance = kwargs["ti"]
    response = task_instance.xcom_pull(task_ids="task4")
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
    "extract_data_from_source",
    schedule_interval=None,
    description="Extract data from source(youtube, bobae, clien).",
) as dag:
    CAR_NAME = "투싼"

    collect_target_video = LambdaInvokeFunctionOperator(
        task_id="collect_target_video",
        function_name="collect_target_video",
        aws_conn_id="aws_default",
        payload={"input_date": "2025-01", "car_name": CAR_NAME},
    )
    collect_target_bobae = LambdaInvokeFunctionOperator(
        task_id="collect_target_bobae",
        function_name="collect_target_bobae",
        aws_conn_id="aws_default",
        payload={"input_date": "2025-01", "car_name": CAR_NAME},
    )
    collect_target_clien = LambdaInvokeFunctionOperator(
        task_id="collect_target_clien",
        function_name="collect_target_clien",
        aws_conn_id="aws_default",
        payload={"input_date": "2025-01", "car_name": CAR_NAME},
    )

    # crawl youtube batch 5
    crawl_youtube = [
        LambdaInvokeFunctionOperator(
            task_id=f"crawl_youtube_{i}",
            function_name="crawl_youtube",
            aws_conn_id="aws_default",
            payload={"input_date": "2025-01", "car_name": CAR_NAME, "page": i},
        )
        for i in range(1, 6)
    ]

    crawl_bobae = LambdaInvokeFunctionOperator(
        task_id="crawl_bobae",
        function_name="crawl_bobae",
        aws_conn_id="aws_default",
        payload={"input_date": "2025-01", "car_name": CAR_NAME},
    )
    crawl_clien = LambdaInvokeFunctionOperator(
        task_id="crawl_clien",
        function_name="crawl_clien",
        aws_conn_id="aws_default",
        payload={"input_date": "2025-01", "car_name": CAR_NAME},
    )

    success_callback = PythonOperator(
        task_id="success_callback",
        python_callable=success_callback,
    )

    collect_target_video >> crawl_youtube
    collect_target_bobae >> crawl_bobae
    collect_target_clien >> crawl_clien

    crawl_youtube >> success_callback
    crawl_bobae >> success_callback
    crawl_clien >> success_callback
