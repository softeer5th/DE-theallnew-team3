import argparse
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, split, udf
from pyspark.sql.functions import lower, regexp_replace, trim, length
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import uuid

BUCKET_NAME = "the-all-new-bucket"


# UUID 생성 UDF 정의
def get_uuid():
    return str(uuid.uuid4())


get_uuid_udf = udf(get_uuid, StringType())  # Spark UDF로 변환


def to_flattend(spark, df, source, car_name):

    if df is None:
        print(f"[WARNING] {source} 데이터가 비어 있어 변환을 건너뜁니다.")
        return None, None

    # `uuid()`로 `post_id` 생성
    df_with_post_id = df.withColumn("post_id", F.expr("uuid()"))

    # explode() 적용 전에 comment_count > 0 필터링
    df_filtered = df_with_post_id.filter(F.col("comment_count") > 0)

    # explode() 적용
    df_comments = df_filtered.withColumn("comment", F.explode("comments"))

    # 필요한 컬럼만 선택하여 메모리 사용량 절감
    df_comments = df_comments.select(
        F.expr("uuid()").alias("comment_id"),  # UDF 없이 Spark 내장 함수 사용
        F.col("comment.comment_nickname").alias("author"),
        F.col("comment.comment_content").alias("content"),
        F.col("comment.comment_date").alias("timestamp"),
        F.col("comment.comment_like_count").alias("like_cnt"),
        F.col("comment.comment_dislike_count").alias("dislike_cnt"),
        "post_id",
    )

    df_posts = df_with_post_id.drop("comments")
    df_posts = df_posts.withColumn("car_name", F.lit(car_name))
    df_posts = df_posts.withColumn("source", F.lit(source))

    df_posts = df_posts.select(
        "post_id",
        "title",
        F.col("nickname").alias("author"),
        "article",
        F.col("date").alias("timestamp"),
        F.col("like_count").alias("like_cnt"),
        F.col("dislike_count").alias("dislike_cnt"),
        F.col("view_count").alias("view_cnt"),
        F.col("comment_count").alias("comment_cnt"),
        "car_name",
        "source",
    )
    return df_posts, df_comments


def make_sentence(df, type):
    try:
        if df is None:
            print(f"[WARNING] {type} 데이터가 비어 있어 변환을 건너뜁니다.")
            return None

        if type == "post":
            df_title = df.select(
                get_uuid_udf().alias("sentence_id"),
                col("post_id"),
                lit(None).cast("string").alias("comment_id"),
                lit("post").alias("type"),
                col("title").alias("text"),
            )
            df_article = df.select(
                col("post_id"),
                lit(None).alias("comment_id"),
                lit("post").alias("type"),
                explode(split(col("article"), "\n")).alias("text"),
            ).withColumn("sentence_id", get_uuid_udf())

            # 컬럼 순서 변경 (title과 article 컬럼 순서 동일해지도록)
            df_article = df_article.select(
                "sentence_id", "post_id", "comment_id", "type", "text"
            )
            df = df_title.union(df_article)

        else:  # if type == "comment"
            df = df.select(
                get_uuid_udf().alias("sentence_id"),
                col("post_id"),
                col("comment_id"),
                lit("comment").alias("type"),
                col("content").alias("text"),
            )

        return df

    except Exception as e:
        print(f"데이터 변환 중 오류 발생 ({type}): {e}")
        print(traceback.format_exc())
        return None


# 텍스트 정제
def to_cleaned(df):
    try:
        df = df.filter(length(col("text")) > 10)
        # df = df.withColumn("text", lower(col("text")))
        df = df.filter(col("text").rlike(".*[가-힣].*"))
        df = df.withColumn("text", regexp_replace(col("text"), r"http\S+", ""))
        df = df.withColumn("text", regexp_replace(col("text"), r"https\S+", ""))
        df = df.withColumn("text", regexp_replace(col("text"), r"@\S+", ""))
        df = df.withColumn("text", regexp_replace(col("text"), r"#\S+", ""))
        df = df.withColumn("text", regexp_replace("text", r'[",\[\]]+', ""))
        df = df.withColumn("text", regexp_replace(col("text"), r"([.!?,~])\1+", "$1"))
        df = df.withColumn(
            "text",
            regexp_replace(
                col("text"),
                r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF\u00A0]",
                "",
            ),
        )
        df = df.withColumn("text", trim(regexp_replace(col("text"), r"\s+", " ")))
        df = df.filter(length(col("text")) > 10)
        return df

    except Exception as e:
        print(f"텍스트 정제 중 오류 발생: {e}")
        return None


def process_text(year, month, car_name):
    spark = SparkSession.builder.appName("Process Text").getOrCreate()

    # s3에서 json 파일 읽어오기
    df_youtube = spark.read.json(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/youtube_raw_*.json",
        multiLine=True,
    )
    df_bobae = spark.read.json(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/bobae_raw.json",
        multiLine=True,
    )
    df_clien = spark.read.json(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/clien_raw.json",
        multiLine=True,
    )

    # json 데이터를 post, comment로 분리하기
    youtube_post, youtube_comment = to_flattend(spark, df_youtube, "youtube", car_name)
    bobae_post, bobae_comment = to_flattend(spark, df_bobae, "bobae", car_name)
    clien_post, clien_comment = to_flattend(spark, df_clien, "clien", car_name)

    # data 합치기
    df_post = youtube_post.union(bobae_post).union(clien_post)
    df_comment = youtube_comment.union(bobae_comment).union(clien_comment)

    # parquet 저장
    df_post.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/post_data"
    )
    df_comment.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/comment_data"
    )

    # post data를 sentence data로 변환
    df_post = make_sentence(df_post, "post")
    # comment data를 sentence data로 변환
    df_comment = make_sentence(df_comment, "comment")

    # 한 sentence data로 합치기
    df = df_post.union(df_comment)
    # sentence data를 rule based 로 정제(특수 문자 제거 등)
    df = to_cleaned(df)

    df = df.repartition(10)
    df.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/sentence_data"
    )

    spark.stop()


if __name__ == "__main__":
    print("Starting process_text")
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="The year of the data.")
    parser.add_argument("--month", help="The month of the data.")
    parser.add_argument("--car_name", help="The name of the car.")
    args = parser.parse_args()

    print(f"Processing {args.car_name} data for {args.year}-{args.month}")
    process_text(args.year, args.month, args.car_name)
