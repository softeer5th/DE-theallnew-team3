import os

from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator


S3_BUCKET = "the-all-new-bucket"

copy_params = [
    {"TABLE_NAME": "tb_posts", "S3_KEY": "post_data/"},
    {"TABLE_NAME": "tb_comments", "S3_KEY": "comment_data/"},
    {"TABLE_NAME": "tb_sentences", "S3_KEY": "sentence_data/"},
    {"TABLE_NAME": "tb_keywords", "S3_KEY": "classified/"},
]

with DAG(
    dag_id="ELT_DAG_S3_to_Redshift",
    description="DAG for ELT process from S3 to Redshift",
    template_searchpath=[os.path.join(os.path.dirname(__file__), 'sql')]
) as dag:
    
    init_staging_task = RedshiftDataOperator(
        task_id=f"Task-initialize-Redshift-staging-table",
        sql="init_staging.sql",
        workgroup_name="the-all-new-workgroup",
        region_name="ap-northeast-2",
        database="dev",
        aws_conn_id='aws_default',
    )

    copy_to_staging_tasks = []
    for car_name in ['Casper']:
        for copy_param in copy_params:
            s3_key = (
                f"""{car_name}/{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%Y') }}}}/{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%m') }}}}/{{{{ macros.ds_format(macros.ds_add(ds, -1), '%Y-%m-%d', '%d') }}}}/{copy_param['S3_KEY']}"""
            )

            copy_to_staging_task = S3ToRedshiftOperator(
                task_id=f"{car_name}-Task-Copy-staging.{copy_param['TABLE_NAME']}",
                schema="staging",
                table=copy_param["TABLE_NAME"],
                s3_bucket=S3_BUCKET,
                s3_key=s3_key,
                copy_options=[
                    "FORMAT AS PARQUET",
                ],
                redshift_conn_id='redshift_default',
                aws_conn_id='aws_default'
            )
            copy_to_staging_tasks.append(copy_to_staging_task)

    upsert_staging_to_mart_task = RedshiftDataOperator(
        task_id="Task-upsert-Redshift-mart-table",
        sql="upsert_load_to_mart.sql",
        workgroup_name="the-all-new-workgroup",
        region_name="ap-northeast-2",
        database="dev",
        aws_conn_id='aws_default',
    )

    append_staging_to_mart_task = RedshiftDataOperator(
        task_id="Task-append-Redshift-mart-table",
        sql="append_load_to_mart.sql",
        workgroup_name="the-all-new-workgroup",
        region_name="ap-northeast-2",
        database="dev",
        aws_conn_id='aws_default',
    )

    clear_staging_task = RedshiftDataOperator(
        task_id="Task-clear-Redshift-staging-table",
        sql="clear_staging.sql",
        workgroup_name="the-all-new-workgroup",
        region_name="ap-northeast-2",
        database="dev",
        aws_conn_id='aws_default',
    )

    init_staging_task >> copy_to_staging_tasks

    for copy_task in copy_to_staging_tasks:
        copy_task >> [upsert_staging_to_mart_task, append_staging_to_mart_task]

    [upsert_staging_to_mart_task, append_staging_to_mart_task] >> clear_staging_task