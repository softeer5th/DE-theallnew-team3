from pyspark import SparkConf
from pyspark.sql import SparkSession
import argparse

def getSparkSession() -> SparkSession : 
    conf = SparkConf().set("spark.port.maxRetries", "50")
    spark = SparkSession.builder.config(conf=conf) \
        .appName("GetS3FiletoLocal") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.4,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .getOrCreate()
    return spark

def getFileFromS3(spark: SparkSession, s3_path: str):
    df = spark.read.parquet(s3_path)
    return df

def main(s3_path):
    # SparkSession 생성
    spark = getSparkSession()
    if spark is None:
        raise Exception("SparkSession 생성 실패! 환경설정을 확인하세요.")

    print("SparkSession successfully created!")

    s3_prefix = "*.parquet"

    # 전체 S3 경로
    s3_files = f"{s3_path}/{s3_prefix}"

    # S3에서 데이터 가져오기
    df = getFileFromS3(spark, s3_files)

    # 데이터 출력
    df.show()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('data_source', help="The S3 URI of the input folder containing JSON files.")
    args = parser.parse_args()

    main(args.data_source)