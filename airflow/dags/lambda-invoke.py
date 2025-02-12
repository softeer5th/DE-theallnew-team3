# invoke lambda function dag

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.operators.lambda_function import (
    AWSLambdaInvokeFunctionOperator,
)
from datetime import datetime

with DAG(
    "minjae-test", schedule_interval="0 12 * * *", start_date=datetime(2025, 1, 1)
) as dag:
    task1 = BashOperator(task_id="task1", bash_command='echo "Hello World"')
    task2 = BashOperator(task_id="task2", bash_command='echo "I\'m Minjae"')
    time = datetime.now()
    task3 = BashOperator(task_id="task3", bash_command=f'echo "time: {time}"')
    task4 = AWSLambdaInvokeFunctionOperator(
        task_id="task4",
        function_name="minjae-test",
        aws_conn_id="aws_default",
    )

    [task1, task2] >> task3 >> task4
