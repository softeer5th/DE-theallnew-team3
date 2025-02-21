from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator


S3_BUCKET = "the-all-new-bucket"

copy_params = [
    {"TABLE_NAME": "tb_posts", "S3_KEY": "post_data/"},
    {"TABLE_NAME": "tb_comments", "S3_KEY": "comment_data/"},
    {"TABLE_NAME": "tb_sentences", "S3_KEY": "sentence_data/"},
    {"TABLE_NAME": "tb_keywords", "S3_KEY": "classified/"},
]

with DAG(
    dag_id="copy",
    description="DAG for ELT process from S3 to Redshift"# SQL 템플릿 파일들의 경로 설정
) as dag:
    copy_to_staging_tasks = []
    for car_name in ["Casper"]:
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
                aws_conn_id="redshift_default",
            )
            copy_to_staging_tasks.append(copy_to_staging_task)

    copy_to_staging_tasks