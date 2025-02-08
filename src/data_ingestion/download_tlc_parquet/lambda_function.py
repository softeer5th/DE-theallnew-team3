import os
import logging

import requests
import boto3

s3_client = boto3.client('s3')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet'
    tmp_parquet = "/tmp/yellow_tripdata_2024-01.parquet"
    key = "yellow_tripdata_2024-01.parquet"
    bucket = 'simple-s3-jy'

    logger.info("파일 다운로드 시작: %s", url)

    try:
        with requests.get(url, stream=True) as response:
            response.raise_for_status()
            with open(tmp_parquet, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        logger.info("파일 다운로드 완료: %s", tmp_parquet)
        
        s3_client.upload_file(tmp_parquet, bucket, key)
        os.unlink(tmp_parquet)
        return {
                'statusCode': 200,
                'body': "파일 다운로드 완료"
            }
    except requests.exceptions.RequestException as e:
        logger.error("파일 다운로드 중 오류 발생: %s", e)
        return {
            'statusCode': 500,
            'body': f"파일 다운로드 중 오류 발생: {e}"
        }