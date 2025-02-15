import argparse
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

        print("💾 데이터 저장 중...")
        df.write.parquet(output_s3_path, mode="overwrite")
        print(f"변환 완료! 결과 저장: {output_s3_path}")

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
