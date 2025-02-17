from airflow.decorators import task
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.s3 import S3ListOperator
import logging

logger = logging.getLogger(__name__)


SPARK_STEPS = [
    {
        "Name": "Process Text",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://the-all-new-bucket/py/process_text.py",
                "--year",
                "2025",
                "--month",
                "01",
                "--car_name",
                "그랜저",
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


with DAG(
    "json_to_parquet",
    schedule_interval=None,
    tags=["etl"],
    description="ETL: json to parquet",
) as dag:
    # TODO: 차종은 어떻게 처리?
    CAR_NAME = "그랜저"
    # TODO: Schedule 기준으로 변경
    INPUT_DATE = "2025-01"
    year, month = INPUT_DATE.split("-")[:2]

    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="emr_create_job_flow",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )
    add_steps = EmrAddStepsOperator(
        task_id="emr_add_steps",
        job_flow_id=create_emr_cluster.output,
        steps=SPARK_STEPS,
    )

    add_steps.wait_for_completion = True

    wait_for_steps = EmrStepSensor(
        task_id="wait_for_steps",
        job_flow_id=create_emr_cluster.output,
        step_id=get_step_id(add_steps.output),
    )
    target_files = S3ListOperator(
        task_id="get_target_file_list",
        bucket="the-all-new-bucket",
        prefix=f"{CAR_NAME}/{year}/{month}/sentence_data/",
        delimiter="/",
    )

    create_emr_cluster >> add_steps >> wait_for_steps >> target_files
