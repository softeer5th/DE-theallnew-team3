import json
import boto3
from botocore.exceptions import ClientError

redshift_client = boto3.client('redshift-data')

REDSHIFT_CLUSTER_ID = 'simple-reshift-jy'
REDSHIFT_DB_NAME = 'tlc_nyc'
REDSHIFT_ROLE_ARN = 'arn:aws:iam::442426874570:role/service-role/AmazonRedshift-CommandsAccessRole-20250210T082001'


def lambda_handler(event, context):
    s3_bucket = 'simple-s3-jy'
    s3_key = 'yellow_tripdata_2024-01.parquet_transform/'

    copy_command = f"""
    COPY yellow_taxi_trip_records
    FROM 's3://{s3_bucket}/{s3_key}'
    IAM_ROLE '{REDSHIFT_ROLE_ARN}'
    FORMAT AS PARQUET
    """

    try:
        response = redshift_client.execute_statement(
            ClusterIdentifier=REDSHIFT_CLUSTER_ID,
            Database=REDSHIFT_DB_NAME,
            SecretArn='arn:aws:secretsmanager:ap-northeast-2:442426874570:secret:redshift!simple-reshift-jy-reshift-jy-Sp6iau',
            Sql=copy_command
        )
        
        # 작업 결과 확인
        statement_id = response['Id']
        print(f"Executed COPY statement, StatementId: {statement_id}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'COPY command executed successfully, StatementId: {statement_id}')
        }

    except ClientError as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error executing COPY command: {str(e)}")
        }