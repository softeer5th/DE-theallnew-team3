import os
import boto3 #다운받기
import requests
from datetime import datetime

def download_tlc_data(bucket_name: str):
    """
    뉴욕 TLC 데이터를 다운로드하고 S3에 업로드하는 Lambda 함수
    """
    
    #base_url = "https://www.nyc.gov/assets/tlc/downloads/pdf/"
    #file_name = f"yellow_tripdata_{year}-{month:02d}.csv"
    file_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"

    # 파일 이름 추출
    file_name = file_url.split("/")[-1]  # yellow_tripdata_2024-01.parquet
    local_path = f"/tmp/{file_name}"
    
    # 파일 다운로드
    response = requests.get(file_url)
    if response.status_code != 200:
        raise Exception(f"파일 다운로드 실패: {file_url}")
    
    with open(local_path, 'wb') as file:
        file.write(response.content)
    
    # S3에 업로드
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket_name, file_name)
    
    print(f"파일 {file_name}이 S3 버킷 {bucket_name}에 업로드되었습니다.")
    
    # return {
    #     "statusCode": 200,
    #     "body": f"파일 {file_url}이 S3 버킷 {bucket_name}에 업로드되었습니다."
    # }

def lambda_handler():
    """
    AWS Lambda 핸들러
    """
    #year = event.get("year", datetime.now().year)
    #month = event.get("month", datetime.now().month - 1)  # 기본값: 이전 달 데이터
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    print('s3 name:',bucket_name)
    
    if not bucket_name:
        raise ValueError("환경 변수 S3_BUCKET_NAME이 설정되지 않았습니다.")
    
    download_tlc_data(bucket_name)


# 로컬 실행 코드
if __name__ == "__main__":
    os.environ["S3_BUCKET_NAME"] = "S3 버킷 이름" 
    lambda_handler()
