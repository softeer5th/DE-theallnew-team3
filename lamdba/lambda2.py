import json
import urllib.parse
import boto3

print("Loading function")

s3 = boto3.client("s3")
emr = boto3.client("emr", region_name="ap-northeast-2")

CLUSTER_ID = "j-XXXXXXXXXXXXX"  # 실제 EMR 클러스터 ID 입력

def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(event["Records"][0]["s3"]["object"]["key"], encoding="utf-8")

    if not key.endswith(".parquet"):
        print(f"파일 {key}은 Parquet이 아니므로 처리하지 않습니다.")
        return

    print(f"새로운 Parquet 파일 감지: {key}")

    try:
        step = {
            "Name": f"Process {key}",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",  
                    "--deploy-mode", "cluster",  
                    "--jars", "s3://your-bucket/jars/mysql-connector-java-8.0.26.jar",  # JDBC 드라이버 경로
                    "s3://your-bucket/scripts/process_parquet.py",  # Spark 처리 코드
                    f"s3://{bucket}/{key}",  # S3에서 업로드된 Parquet 파일 경로
                ],
            },
        }

        response = emr.add_job_flow_steps(JobFlowId=CLUSTER_ID, Steps=[step])
        print(f"EMR Step 실행됨: {response['StepIds'][0]}")

        return {
            "statusCode": 200,
            "body": f"EMR Step {response['StepIds'][0]} 실행 완료",
        }
    
    except Exception as e:
        print(f"EMR 실행 중 오류 발생: {str(e)}")
        raise e
