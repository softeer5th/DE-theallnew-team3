import argparse
import subprocess
import boto3
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, split
from pyspark.sql.functions import lower, regexp_replace, trim, length
from pyspark.sql import functions as F


def get_spark_session():
    try:
        spark = SparkSession.builder \
            .appName("S3FiletoLocal") \
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.4,com.amazonaws:aws-java-sdk-bundle:1.11.901") \
            .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
            .getOrCreate()
        return spark
    except Exception as e:
        print(f"SparkSession 생성 실패: {e}")
        return None
    
def to_flattend(df, source_type):
    try:
        title_df = df.select(
            col("title").alias("text"),
            col("date"),
            col("view_count"),
            col("like_count"),
            lit(0).alias("dislike_count"),
            lit(source_type).alias("source"),
            lit("title").alias("type"),
        )

        if source_type == "bobae":
            comment_df = df.select(explode(col("comments")).alias("comment")).select(
                col("comment.comment_content").alias("text"),
                col("comment.comment_date").alias("date"),
                lit(0).alias("view_count"),
                col("comment.comment_likeCount").alias("like_count"),
                col("comment.comment_dislikeCount").alias("dislike_count"),
                lit(source_type).alias("source"),
                lit("comment").alias("type"),
            )
        else:
            comment_df = df.select(explode(col("comments")).alias("comment")).select(
                col("comment.comment_content").alias("text"),
                col("comment.comment_date").alias("date"),
                lit(0).alias("view_count"),
                col("comment.comment_like_count").alias("like_count"),
                col("comment.comment_dislike_count").alias("dislike_count"),
                lit(source_type).alias("source"),
                lit("comment").alias("type"),
            )

        article_df = df.select(
            explode(split(col("article"), "\n")).alias("text"),
            col("date"),
            col("view_count"),
            col("like_count"),
            lit(0).alias("dislike_count"),
            lit(source_type).alias("source"),
            lit("article").alias("type"),
        )

        flattened_df = title_df.union(article_df).union(comment_df)
        flattened_df = flattened_df.fillna({"view_count": 0, "like_count": 0, "dislike_count": 0})
        return flattened_df

    except Exception as e:
        print(f"데이터 변환 중 오류 발생 ({source_type}): {e}")
        return None


def add_weight(df):
    try:
        article_or_title_df = df.filter((df["type"] == "article") | (df["type"] == "title"))
        article_or_title_df = article_or_title_df.withColumn(
            "weight",
            F.round(
                F.when(
                    (df["source"] == "youtube"),
                    F.log(F.log(1 + df["view_count"] + 1)) + df["like_count"]
                ).otherwise(
                    F.log(1 + df["view_count"]) + df["like_count"]
                ),
                1
            )
        )

        comments_df = df.filter(df["type"] == "comment")
        comments_df = comments_df.withColumn(
            "weight",
            F.round(df["like_count"] + df["dislike_count"], 1)
        )

        final_df = article_or_title_df.union(comments_df)
        return final_df

    except Exception as e:
        print(f"가중치 계산 중 오류 발생: {e}")
        return None


def to_cleaned(df):
    try:
        df = df.withColumn("text", lower(col("text")))
        df = df.withColumn("text", regexp_replace(col("text"), r"http\S+", ""))
        df = df.withColumn("text", regexp_replace(col("text"), r"https\S+", ""))
        df = df.withColumn("text", regexp_replace(col("text"), r"@\S+", ""))
        df = df.withColumn("text", regexp_replace(col("text"), r"#\S+", ""))
        df = df.withColumn("text", regexp_replace("text", r'[",\[\]]+', ''))
        df = df.withColumn("text", regexp_replace(col("text"), r"([.!?,~])\1+", "$1"))
        df = df.withColumn(
            "text",
            regexp_replace(
                col("text"), r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF\u00A0]", ""
            ),
        )
        df = df.withColumn("text", trim(regexp_replace(col("text"), r"\s+", " ")))
        df = df.withColumn("text", regexp_replace(col("text"), r"투산+", "투싼"))
        df = df.filter(col("text").rlike(".*[가-힣].*"))
        df = df.filter(length(col("text")) > 10)
        return df

    except Exception as e:
        print(f"텍스트 정제 중 오류 발생: {e}")
        return None


def save_parquet_files_v1(df, output_s3_path, num_files=5):
    """
    데이터를 num_files 개수만큼 나누어 S3에 저장하는 함수
    """
    try:
        df_list = df.randomSplit([1.0 / num_files] * num_files)

        s3_client = boto3.client("s3")
        
        # s3a:// 제거 후 버킷과 경로 분리
        bucket_name = output_s3_path.replace("s3a://", "").split("/")[0]
        prefix = "/".join(output_s3_path.replace("s3a://", "").split("/")[1:])

        print("데이터 저장 중...")

        for i, df_part in enumerate(df_list):
            temp_dir = f"/tmp/parquet_part_{i+1}"  # 임시 디렉토리 생성
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)  # 기존 디렉터리 삭제
            os.makedirs(temp_dir)

            # 단일 파일로 저장 (coalesce(1) 사용)
            df_part.coalesce(1).write.mode("overwrite").parquet(temp_dir)

            # Spark가 만든 실제 파일 찾기
            file_name = [f for f in os.listdir(temp_dir) if f.startswith("part-")][0]
            local_file_path = os.path.join(temp_dir, file_name)

            # 올바른 S3 경로로 파일 업로드
            s3_file_path = f"{prefix}/data_part_{i+1}.parquet"
            s3_client.upload_file(local_file_path, bucket_name, s3_file_path)
            print(f"저장 완료: {s3_file_path}")

            # 임시 디렉토리 삭제
            shutil.rmtree(temp_dir)

    except Exception as e:
        print(f"Parquet 저장 중 오류 발생: {e}")


def save_parquet_files_v2(df, output_s3_path, num_files=5):
    try:
        # Split data into smaller partitions
        df_list = df.randomSplit([1.0 / num_files] * num_files)
        print("데이터 저장 중...")

        # S3 paths split
        bucket_name = output_s3_path.replace("s3a://", "").split("/")[0]
        prefix = "/".join(output_s3_path.replace("s3a://", "").split("/")[1:])

        # Iterate over the partitions and save non-empty partitions
        for i, df_part in enumerate(df_list):
            if df_part.count() == 0:  # Skip empty partitions
                print(f"경고: 데이터 파티션 {i+1}이 비어 있습니다. 저장을 건너뜁니다.")
                continue

            temp_dir = f"/tmp/parquet_part_{i+1}"
            if os.path.exists(temp_dir):
                shutil.rmtree(temp_dir)
            os.makedirs(temp_dir)

            # Save the partition as a Parquet file
            df_part.coalesce(1).write.mode("overwrite").parquet(temp_dir)

            # Locate the generated file
            file_name = [f for f in os.listdir(temp_dir) if f.startswith("part-")][0]
            local_file_path = os.path.join(temp_dir, file_name)

            # Set the target S3 path
            s3_file_path = f"s3://{bucket_name}/{prefix}/data_part_{i+1}.parquet"

            print(f"Uploading local file to S3: {local_file_path} -> {s3_file_path}")
            subprocess.run(["aws", "s3", "cp", local_file_path, s3_file_path])

            print(f"Saved: {s3_file_path}")

            shutil.rmtree(temp_dir)  # Clean up temp directory

    except Exception as e:
        print(f"Error while saving Parquet: {e}")


def transform_text(input_s3_path, output_s3_path):
    spark = get_spark_session()
    if spark is None:
        print("SparkSession을 생성하지 못해 종료합니다.")
        return

    try:
        print("데이터 로드 중...")
        df_youtube = spark.read.json(f"{input_s3_path}/youtube_raw_*.json", multiLine=True)
        df_bobae = spark.read.json(f"{input_s3_path}/bobae_raw.json", multiLine=True)
        df_clien = spark.read.json(f"{input_s3_path}/clien_raw.json", multiLine=True)

        df_youtube = to_flattend(df_youtube, "youtube")
        df_bobae = to_flattend(df_bobae, "bobae")
        df_clien = to_flattend(df_clien, "clien")

        if df_youtube is None or df_bobae is None or df_clien is None:
            print("데이터 변환 실패. 종료합니다.")
            return

        df = df_youtube.union(df_bobae).union(df_clien)

        df = add_weight(df)
        if df is None:
            print("가중치 계산 실패. 종료합니다.")
            return

        df = to_cleaned(df)
        if df is None:
            print("데이터 정제 실패. 종료합니다.")
            return

        # 데이터 5개로 나누어 저장
        save_parquet_files_v2(df, output_s3_path, num_files=5)

    except Exception as e:
        print(f"전체 프로세스 중 오류 발생: {e}")

    finally:
        print("Spark 세션 종료")
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('data_source', help="The S3 URI of the input folder containing JSON files.")
    parser.add_argument('output_uri', help="The S3 URI where the output Parquet files should be saved.")
    args = parser.parse_args()

    transform_text(args.data_source, args.output_uri)
