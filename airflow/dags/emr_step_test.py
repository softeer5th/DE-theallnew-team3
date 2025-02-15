from airflow.decorators import task
from airflow import DAG
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
import logging

logger = logging.getLogger(__name__)


SPARK_STEPS = [
    {
        "Name": "Spark Step Test1",
        "ActionOnFailure": "TERMINATE_CLUSTER",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode",
                "cluster",
                "s3://the-all-new-bucket/py/job2.py",
                "--data_source",
                "s3://the-all-new-bucket/yellow_tripdata_2024-01.parquet",
                "--output_uri",
                "s3://the-all-new-bucket/out",
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
    "LogUri": "s3://the-all-new-logs/emr",
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


# Test DAG
with DAG(
    "example.emr_step_test",
    schedule_interval=None,
    tags=["test"],
    description="Test DAG: EMR test",
) as dag:
    create_emr_cluster = EmrCreateJobFlowOperator(
        task_id="emr_create_job_flow",
        aws_conn_id="aws_default",
        job_flow_overrides=JOB_FLOW_OVERRIDES,
    )
    add_steps = EmrAddStepsOperator(
        task_id="emr_add_steps",
        job_flow_id=create_emr_cluster.output,
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        # execution_role_arn=''
    )

    add_steps.wait_for_completion = True

    wait_for_steps = EmrStepSensor(
        task_id="wait_for_steps",
        job_flow_id=create_emr_cluster.output,
        aws_conn_id="aws_default",
        step_id=get_step_id(add_steps.output),
    )

    create_emr_cluster >> add_steps >> wait_for_steps
