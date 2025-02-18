
from airflow import DAG
from airflow.models import Variable
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

# default_args = {
#     "owner": "airflow",
#     "start_date": datetime(2023, 1, 1),
# }

# 외부에서 전달받거나 Airflow Variables 혹은 DAG run config 등을 통해 설정할 수 있는 파라미터 목록
task_params = [
    {"TABLE_NAME": "posts_tb", "S3_KEY": "post_data/"},
    {"TABLE_NAME": "comments_tb", "S3_KEY": "comment_data/"},
    {"TABLE_NAME": "sentence_tb", "S3_KEY": "sentence_data/"}
]
car_names = Variable.get("car_name", deserialize_json=True)
S3_BUCKET = "the-all-new-bucket"

with DAG(
    dag_id="s3_to_redshift",
    #default_args=default_args,
    # schedule_interval=timedelta(days=1),
    description="Copy data from s3 to Redshift"
) as dag:
    for car_name in car_names:
        tasks = []
        for params in task_params:
            s3_object = s3_object = f"{car_name}/{{ data_interval_start.strftime('%Y') }}/{{ data_interval_start.strftime('%m') }}/{params['S3_KEY']}"
            task = S3ToRedshiftOperator(
                task_id=f"copy_data_from_s3_{params['TABLE_NAME']}",
                schema="public",
                table=params["TABLE_NAME"],
                s3_bucket=S3_BUCKET,
                s3_key=params["S3_KEY"],
                copy_options=[
                    "FORMAT AS PARQUET",
                ],
                aws_conn_id="redshift_default",
            )
            tasks.append(task)
