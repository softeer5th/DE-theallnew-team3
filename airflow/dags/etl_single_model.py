from airflow.decorators import task
import logging
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator,
)
from datetime import datetime, timedelta
from constant.car_data import CAR_TYPE_PARAM, CARS
from common.slack import slack_info_message, slack_handle_task_failure

logger = logging.getLogger(__name__)


def generate_step(year, month, day, car_name):
    return [
        {
            "Name": f"Process {car_name} at {year}-{month}-{day}",
            "ActionOnFailure": "TERMINATE_CLUSTER",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--deploy-mode",
                    "cluster",
                    "s3://the-all-new-bucket/py/process_text.py",
                    "--year",
                    year,
                    "--month",
                    month,
                    "--day",
                    day,
                    "--car_name",
                    car_name,
                ],
            },
        }
    ]


JOB_FLOW_OVERRIDES = {
    "Name": "EMR Test",
    "ReleaseLabel": "emr-7.7.0",
    "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "InstanceType": "m4.large",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceCount": 1,
            },
            {
                "Name": "Worker nodes",
                "InstanceType": "m4.large",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceCount": 2,
            },
        ],
        "Ec2SubnetId": "subnet-06f1e9f77ff80e755",
        "KeepJobFlowAliveWhenNoSteps": False,
        "TerminationProtected": False,
    },
    "LogUri": "s3://the-all-new-logs/elasticmapreduce",
    "Tags": [{"Key": "for-use-with-amazon-emr-managed-policies", "Value": "true"}],
    "VisibleToAllUsers": True,
    "JobFlowRole": "DE_3_EMR_Instance_Role",
    "ServiceRole": "DE_3_EMR_Service_Role",
}


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

    # 이전달 데이터 수집하기
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


with DAG(
    "etl.single_model",
    schedule=None,
    description="ETL: single model",
    tags=["etl", "single"],
    params={"car_type": CAR_TYPE_PARAM},
    on_failure_callback=slack_handle_task_failure,
) as dag:
    target_car = PythonOperator(
        task_id="get_target_car",
        python_callable=get_params,
    )

    PAYLOAD_JSON = {
        "input_date": "{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y') }}-{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%m') }}-{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%d') }}",
        "car_name": "{{ task_instance.xcom_pull(task_ids='get_target_car')['car_name'] }}",
        "search_keyword": "{{ task_instance.xcom_pull(task_ids='get_target_car')['alias'][0] }}",
    }
    PAYLOAD = json.dumps(PAYLOAD_JSON)

    collect_target_video = LambdaInvokeFunctionOperator(
        task_id="collect_target_video",
        function_name="collect_target_video",
        payload=PAYLOAD,
    )
    collect_target_bobae = LambdaInvokeFunctionOperator(
        task_id="collect_target_bobae",
        function_name="collect_target_bobae",
        payload=PAYLOAD,
    )
    collect_target_clien = LambdaInvokeFunctionOperator(
        task_id="collect_target_clien",
        function_name="collect_target_clien",
        payload=PAYLOAD,
    )

    BATCH_SIZE = 10
    crawl_youtube = LambdaInvokeFunctionOperator.partial(
        task_id="crawl_youtube",
        function_name="crawl_youtube",
    ).expand(
        payload=[
            json.dumps({**PAYLOAD_JSON, "page": i}) for i in range(1, BATCH_SIZE + 1)
        ]
    )

    crawl_bobae = LambdaInvokeFunctionOperator(
        task_id="crawl_bobae",
        function_name="crawl_bobae",
        payload=PAYLOAD,
    )
    crawl_clien = LambdaInvokeFunctionOperator(
        task_id="crawl_clien",
        function_name="crawl_clien",
        payload=PAYLOAD,
    )

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
        task_id="invoke_lambda", function_name="classify-sentence"
    ).expand(
        payload=generate_payload.output.map(lambda x: json.dumps(x, ensure_ascii=False))
    )

    send_etl_done_message = slack_info_message(
        message="ETL 완료했어요!!!", dag=dag, task_id="send_etl_done_message"
    )

    target_car >> [
        collect_target_video,
        collect_target_bobae,
        collect_target_clien,
    ]

    collect_target_video >> crawl_youtube
    collect_target_bobae >> crawl_bobae
    collect_target_clien >> crawl_clien

    [crawl_youtube, crawl_bobae, crawl_clien] >> send_crawl_all_success_message

    [crawl_youtube, crawl_bobae, crawl_clien] >> create_emr_cluster

    (
        create_emr_cluster
        >> add_steps
        >> wait_for_steps
        >> get_target_files
        >> generate_payload
        >> invoke_lambda
        >> send_etl_done_message
    )
