from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2023, 1, 1),
# }

task_params = [
    {"TABLE_NAME": "tb_posts", "S3_KEY": "post_data/"},
    {"TABLE_NAME": "tb_comments", "S3_KEY": "comment_data/"},
    {"TABLE_NAME": "tb_sentences", "S3_KEY": "sentence_data/"},
    {"TABLE_NAME": "tb_keywords", "S3_KEY": "classified/"},
]
car_names = Variable.get("car_name", deserialize_json=True)
S3_BUCKET = "the-all-new-bucket"

with DAG(
    dag_id="s3_to_redshift",
    # default_args=default_args,
    # schedule_interval=timedelta(days=1),
    description="Copy Data in s3 to Redshif staging tables",
) as dag:
    for car_name in car_names:
        tasks = []
        for params in task_params:
            s3_key = (
                 f"{car_name}/{{ data_interval_start.strftime('%Y') }}/{{ data_interval_start.strftime('%m') }}/{{ data_interval_start.strftime('%d') }}/{params['S3_KEY']}"
            )
            task = S3ToRedshiftOperator(
                task_id=f"copy_data_from_s3_{car_name}_{params['TABLE_NAME']}",
                schema="staging",
                table=params["TABLE_NAME"],
                s3_bucket=S3_BUCKET,
                s3_key=s3_key,
                copy_options=[
                    "FORMAT AS PARQUET",
                ],
                aws_conn_id="redshift_default",
            )
            tasks.append(task)
