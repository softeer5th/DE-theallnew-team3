import argparse
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, split, udf
from pyspark.sql.functions import lower, regexp_replace, trim, length
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    ArrayType,
    IntegerType,
)

BUCKET_NAME = "the-all-new-bucket"

INPUT_SCHEMA = StructType(
    [
        StructField("car_name", StringType()),
        StructField("source", StringType()),
        StructField("id", StringType()),
        StructField("title", StringType()),
        StructField("nickname", StringType()),
        StructField("article", StringType()),
        StructField("view_count", IntegerType()),
        StructField("like_count", IntegerType()),
        StructField("dislike_count", IntegerType()),
        StructField("date", IntegerType()),
        StructField("comment_count", IntegerType()),
        StructField(
            "comments",
            ArrayType(
                StructType(
                    [
                        StructField("comment_content", StringType()),
                        StructField("comment_nickname", StringType()),
                        StructField("comment_date", IntegerType()),
                        StructField("comment_like_count", IntegerType()),
                        StructField("comment_dislike_count", IntegerType()),
                    ]
                )
            ),
        ),
    ]
)


def get_timestamp(year, month, day):
    # 한국 시간(KST) 기준의 타임존 설정
    KST = timezone(timedelta(hours=9))
    year = int(year)
    month = int(month)
    day = int(day)

    # 해당 날짜의 시작(00:00:00)과 끝(23:59:59)을 KST 기준으로 설정
    start_dt = datetime(year, month, day, 0, 0, 0, tzinfo=KST)
    end_dt = datetime(year, month, day, 23, 59, 59, tzinfo=KST)

    # UTC 타임스탬프로 변환
    start_timestamp = int(start_dt.timestamp())  # 초 단위 정수 변환
    end_timestamp = int(end_dt.timestamp())

    return start_timestamp, end_timestamp


def filter_missing_post(df):
    df = df.dropna()
    df = df.filter(F.col("like_cnt") >= 0)
    df = df.filter(F.col("dislike_cnt") >= 0)
    df = df.filter(F.col("view_cnt") >= 0)
    df = df.filter(F.col("comment_cnt") >= 0)
    return df


def filter_missing_comment(df):
    # just skip missing comment
    # 굳이 필요하다면 filter_missing_post에서 comments 내부를 검사하는 것이 좋을 듯.
    pass


def seperate_post_and_comment(df):
    # `uuid()`로 `post_uuid` 생성
    post_df = df.withColumn("post_uuid", F.expr("uuid()"))

    # explode() 적용 전에 comment_count > 0 필터링
    comment_df = post_df.filter(F.col("comment_count") > 0)

    # explode() 적용.
    comment_df = comment_df.withColumn("comment", F.explode("comments"))

    # rename columns
    comment_df = comment_df.select(
        "post_uuid",
        F.expr("uuid()").alias("comment_uuid"),
        F.concat(
            F.col("id"),
            F.lit("_"),
            F.hash(
                F.col("comment.comment_nickname"), F.col("comment.comment_content")
            ).cast("string"),
        ).alias("comment_id"),
        F.col("comment.comment_nickname").alias("author"),
        F.col("comment.comment_content").alias("content"),
        F.col("comment.comment_date").alias("create_timestamp"),
        F.col("comment.comment_like_count").alias("like_cnt"),
        F.col("comment.comment_dislike_count").alias("dislike_cnt"),
    )

    post_df = post_df.drop("comments")

    # rename columns
    post_df = post_df.select(
        "post_uuid",
        F.col("id").alias("post_id"),
        "title",
        F.col("nickname").alias("author"),
        "article",
        F.col("date").alias("create_timestamp"),
        F.col("like_count").alias("like_cnt"),
        F.col("dislike_count").alias("dislike_cnt"),
        F.col("view_count").alias("view_cnt"),
        F.col("comment_count").alias("comment_cnt"),
        "car_name",
        "source",
    )
    return post_df, comment_df


def explode_post(post_df):
    title_sentence_df = post_df.select(
        col("post_uuid"),
        lit(None).alias("comment_uuid"),
        F.expr("uuid()").alias("sentence_uuid"),
        lit("post").alias("type"),
        col("title").alias("sentence"),
    )
    article_sentence_df = post_df.select(
        col("post_uuid"),
        lit(None).alias("comment_uuid"),
        F.expr("uuid()").alias("sentence_uuid"),
        lit("post").alias("type"),
        explode(split(col("article"), "\n")).alias("sentence"),
    )

    sentence_df = title_sentence_df.union(article_sentence_df)

    return sentence_df


def explode_comment(comment_df):
    sentence_df = comment_df.select(
        col("post_uuid"),
        col("comment_uuid"),
        F.expr("uuid()").alias("sentence_uuid"),
        lit("comment").alias("type"),
        col("content").alias("sentence"),
    )
    return sentence_df


def clean_sentence(df):
    df = df.filter(length(col("sentence")) > 10)
    df = df.filter(col("sentence").rlike(".*[가-힣].*"))
    df = df.withColumn("sentence", lower(col("sentence")))
    df = df.withColumn("sentence", regexp_replace(col("sentence"), r"http\S+", ""))
    df = df.withColumn("sentence", regexp_replace(col("sentence"), r"https\S+", ""))
    df = df.withColumn("sentence", regexp_replace(col("sentence"), r"@\S+", ""))
    df = df.withColumn("sentence", regexp_replace(col("sentence"), r"#\S+", ""))
    df = df.withColumn("sentence", regexp_replace("sentence", r'[",\[\]]+', ""))
    df = df.withColumn(
        "sentence", regexp_replace(col("sentence"), r"([.!?,~])\1+", "$1")
    )
    df = df.withColumn(
        "sentence",
        regexp_replace(
            col("sentence"),
            r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF\u00A0]",
            "",
        ),
    )
    df = df.withColumn("sentence", trim(regexp_replace(col("sentence"), r"\s+", " ")))
    df = df.filter(length(col("sentence")) > 10)
    return df


def process_text(year, month, day, car_name):
    spark = SparkSession.builder.appName("Process Text").getOrCreate()

    # s3에서 json 파일 읽어오기
    raw_df = spark.read.json(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/raw/*.json",
        multiLine=True,
        schema=INPUT_SCHEMA,
    )

    cleaned_raw_df = filter_missing_post(raw_df)
    failed_raw_df = raw_df.subtract(cleaned_raw_df)

    failed_raw_df.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/failed_raw"
    )

    # raw 데이터를 post, comment로 분리하기
    post_df, comment_df = seperate_post_and_comment(cleaned_raw_df)

    # parquet 저장
    post_df.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/post_data"
    )
    comment_df.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/comment_data"
    )

    # 새로 생긴 데이터만 남기기
    start_timestamp, end_timestamp = get_timestamp(year, month, day)

    comment_df = comment_df.filter(
        (start_timestamp <= col("create_timestamp"))
        & (col("create_timestamp") <= end_timestamp)
    )
    post_df = post_df.filter(
        (start_timestamp <= col("create_timestamp"))
        & (col("create_timestamp") <= end_timestamp)
    )

    # sentence 추출
    post_sentence_df = explode_post(post_df)
    comment_sentence_df = explode_comment(comment_df)

    sentence_df = post_sentence_df.union(comment_sentence_df)

    # sentence data를 rule based 로 정제(특수 문자 제거 등)
    sentence_df = clean_sentence(sentence_df)

    sentence_df.repartition(10).write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/sentence_data"
    )

    spark.stop()


if __name__ == "__main__":
    print("Starting process_text")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="The year of the data.")
    parser.add_argument("--month", help="The month of the data.")
    parser.add_argument("--day", help="The day of the data.")
    parser.add_argument("--car_name", help="The name of the car.")
    args = parser.parse_args()

    print(f"Processing {args.car_name} data for {args.year}-{args.month}-{args.day}")
    process_text(args.year, args.month, args.day, args.car_name)
