from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

TABLE_NAME='target_reactions_simple'
S3_BUCKET='the-all-new-bucket'  # TODO Parameter를 사용하도록 변경
S3_OBJECT='local_transformed.csv'


with DAG(
    "s3_to_redshift_etl_using_s3toreredshift",
    description="ETL 파이프라인: S3 데이터를 Redshift로 복사",
) as dag:

    copy_data = S3ToRedshiftOperator(
        task_id="copy_data_from_s3",
        schema="public",
        table=TABLE_NAME,
        s3_bucket=S3_BUCKET,
        s3_key=S3_OBJECT,
        copy_options=[
            "DATEFORMAT AS 'YYYY-MM-DD HH24:MI:SS'",
            "FORMAT AS CSV",
            "IGNOREHEADER 1"
        ],
        aws_conn_id="redshift_default",
    )