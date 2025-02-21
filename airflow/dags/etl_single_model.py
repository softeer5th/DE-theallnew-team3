from airflow.decorators import task
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from datetime import datetime, timedelta
from constant.car_data import CAR_TYPE_PARAM, CARS
from common.slack import (
    slack_info_message,
    slack_handle_task_failure,
    slack_warning_message,
)
from constant.emr_config import JOB_FLOW_OVERRIDES, generate_step

logger = logging.getLogger(__name__)


@task.branch()
def branch_failed(target_task_id, on_success_task_ids, on_failure_task_ids, **kwargs):
    ti = kwargs["ti"]
    lambda_return_value = ti.xcom_pull(task_ids=target_task_id)["return_value"]
    print(lambda_return_value)

    if lambda_return_value.get("failed"):
        return on_failure_task_ids
    return on_success_task_ids


@task
def get_step_id(step_ids: list):
    return step_ids[0]


def log_emr_result(**kwargs):
    task_instance = kwargs["ti"]
    response = task_instance.xcom_pull(task_ids="emr_step_sensor")
    logger.info(f"EMR Response: {response}")


def generate_payload(**kwargs):
    ti = kwargs["ti"]
    ds = kwargs["ds"]
    params = kwargs["params"]
    files = ti.xcom_pull(task_ids="get_target_file_list")

    parquet_files = [f for f in files if f.endswith(".parquet")]
    parquet_files = [f.split("/")[-1] for f in parquet_files]

    cur_datetime = datetime.strptime(ds, "%Y-%m-%d")
    prev_datetime = cur_datetime - timedelta(days=1)

    payload = [
        {
            "input_date": prev_datetime.strftime("%Y-%m-%d"),
            "car_name": params["car_type"],
            "object_key": f,
        }
        for f in parquet_files
    ]

    return payload


def get_params(**kwargs):
    params = kwargs["params"]
    return CARS[params["car_type"]]


@task.branch
def branch_crawl(source: str, ti):
    result = ti.xcom_pull(task_ids=f"{source}.crawl")
    result = json.loads(result)
    if result.get("failed"):
        return f"{source}.recover"
    return None


@task.branch
def branch_recover(source: str, ti):
    result = ti.xcom_pull(task_ids=f"{source}.recover")
    result = json.loads(result)
    if result.get("failed"):
        return f"{source}.send_warning"
    return None


default_args = {
    "on_failure_callback": slack_handle_task_failure,
}

with DAG(
    "etl.single_model",
    schedule=None,
    description="ETL: single model",
    tags=["etl", "single"],
    params={"car_type": CAR_TYPE_PARAM},
    default_args=default_args,
) as dag:
    target_car = PythonOperator(
        task_id="get_target_car",
        python_callable=get_params,
    )

    PAYLOAD_JSON = {
        "input_date": "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y') }}-{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%m') }}-{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%d') }}",
        "car_name": "{{ task_instance.xcom_pull(task_ids='get_target_car')['car_name'] }}",
        "search_keywords": "{{ task_instance.xcom_pull(task_ids='get_target_car')['alias'] }}",
    }
    PAYLOAD = json.dumps(PAYLOAD_JSON)

    with TaskGroup(group_id="crawl_youtube_task_group") as crawl_youtube_task_group:
        collect_target_video = LambdaInvokeFunctionOperator(
            task_id="collect_target_video",
            function_name="collect_target_video",
            payload=PAYLOAD,
        )

        BATCH_SIZE = 10
        crawl_youtube = LambdaInvokeFunctionOperator.partial(
            task_id="crawl_youtube",
            function_name="crawl_youtube",
            botocore_config={"read_timeout": 600, "connect_timeout": 600},
        ).expand(
            payload=[
                json.dumps({**PAYLOAD_JSON, "page": i})
                for i in range(1, BATCH_SIZE + 1)
            ]
        )
        collect_target_video >> crawl_youtube

    with TaskGroup(
        group_id="clien",
    ) as crawl_clien_task_group:
        clien_collect = LambdaInvokeFunctionOperator(
            task_id="collect",
            function_name="collect_target_clien",
            payload=PAYLOAD,
        )
        clien_crawl = LambdaInvokeFunctionOperator(
            task_id="crawl",
            function_name="crawl_clien",
            payload=PAYLOAD,
        )

        clien_branch_crawl = branch_crawl(source="clien")
        clien_branch_recover = branch_recover(source="clien")

        clien_send_warning = slack_warning_message(
            message="`clien`에서 실패한 URL이 있습니다.",
            dag=dag,
            task_id="send_warning",
        )

        clien_recover = LambdaInvokeFunctionOperator(
            task_id="recover",
            function_name="crawl_clien_recovery",
            payload=PAYLOAD,
        )

        clien_collect >> clien_crawl >> clien_branch_crawl
        clien_branch_crawl >> clien_recover
        clien_recover >> clien_branch_recover
        clien_branch_recover >> clien_send_warning

    with TaskGroup(group_id="bobae") as crawl_bobae_task_group:
        bobae_collect = LambdaInvokeFunctionOperator(
            task_id="collect",
            function_name="collect_target_bobae",
            payload=PAYLOAD,
        )
        bobae_crawl = LambdaInvokeFunctionOperator(
            task_id="crawl",
            function_name="crawl_bobae",
            payload=PAYLOAD,
        )

        bobae_branch_crawl = branch_crawl(source="bobae")
        bobae_branch_recover = branch_recover(source="bobae")

        bobae_send_warning = slack_warning_message(
            message="`bobae`에서 실패한 URL이 있습니다.",
            dag=dag,
            task_id="send_warning",
        )
        bobae_recover = LambdaInvokeFunctionOperator(
            task_id="recover",
            function_name="crawl_bobae_recovery",
            payload=PAYLOAD,
        )

        bobae_collect >> bobae_crawl >> bobae_branch_crawl
        bobae_branch_crawl >> bobae_recover
        bobae_recover >> bobae_branch_recover
        bobae_branch_recover >> bobae_send_warning

    send_crawl_all_success_message = slack_info_message(
        message="크롤링 성공했어요!!!",
        dag=dag,
        task_id="send_crawl_all_success_message",
    )

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="emr_create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        trigger_rule="one_success",
    )

    add_steps = EmrAddStepsOperator(
        task_id="emr_add_steps",
        job_flow_id=create_emr_cluster.output,
        steps=generate_step(
            "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y') }}",
            "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%m') }}",
            "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%d') }}",
            "{{ params.car_type }}",
        ),
    )

    add_steps.wait_for_completion = True

    wait_for_steps = EmrStepSensor(
        task_id="wait_for_steps",
        job_flow_id=create_emr_cluster.output,
        step_id=get_step_id(add_steps.output),
    )

    get_target_files = S3ListOperator(
        task_id="get_target_file_list",
        bucket="the-all-new-bucket",
        prefix="{{ params.car_type }}/{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y') }}/{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%m') }}/{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%d') }}/sentence_data/",
    )

    generate_payload = PythonOperator(
        task_id="generate_payload",
        python_callable=generate_payload,
    )

    invoke_lambda = LambdaInvokeFunctionOperator.partial(
        task_id="invoke_lambda",
        function_name="classify-sentence",
        botocore_config={"read_timeout": 600, "connect_timeout": 600},
    ).expand(
        payload=generate_payload.output.map(lambda x: json.dumps(x, ensure_ascii=False))
    )

    send_etl_done_message = slack_info_message(
        message="ETL 완료했어요!!!", dag=dag, task_id="send_etl_done_message"
    )

    target_car >> [
        crawl_youtube_task_group,
        crawl_bobae_task_group,
        crawl_clien_task_group,
    ]

    [
        crawl_youtube_task_group,
        crawl_bobae_task_group,
        crawl_clien_task_group,
    ] >> send_crawl_all_success_message

    [
        crawl_youtube_task_group,
        crawl_bobae_task_group,
        crawl_clien_task_group,
    ] >> create_emr_cluster

    (
        create_emr_cluster
        >> add_steps
        >> wait_for_steps
        >> get_target_files
        >> generate_payload
        >> invoke_lambda
        >> send_etl_done_message
    )
