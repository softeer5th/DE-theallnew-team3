import argparse
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, split, udf
from pyspark.sql.functions import lower, regexp_replace, trim, length
from pyspark.sql.types import StringType
from pyspark.sql import functions as F
import uuid


# UUID 생성 UDF 정의
def get_uuid():
    return str(uuid.uuid4())


get_uuid_udf = udf(get_uuid, StringType())  # Spark UDF로 변환

BUCKET_NAME = "the-all-new-bucket"


def to_flattend(spark, df, source, car_name):

    try:
        if df is None or df.count() == 0:
            print(f"[WARNING] {source} 데이터가 비어 있어 변환을 건너뜁니다.")
            return None, None

        # 여러 게시글 데이터 처리
        post_data = []
        comment_data = []

        # DataFrame에서 데이터를 추출 (df는 Spark DataFrame)
        posts = df.collect()  # .collect()로 Spark DataFrame에서 데이터를 가져온다.

        for post in posts:
            post_id = str(uuid.uuid4())  # 게시글별 고유 ID 생성

            # 게시글 데이터 저장
            post_data.append(
                (
                    post_id,
                    car_name,
                    source,
                    post["title"],
                    post["nickname"],
                    post["article"],
                    post["date"],
                    post["view_count"],
                    post["like_count"],
                    0,
                    post["comment_count"],
                )
            )  # 0은 dislike_cnt입니다.

            # 댓글 데이터 저장 (각 댓글이 해당 post_id를 참조)
            if post["comment_count"] > 0:  # 'comments'가 있을 경우에만 처리
                for comment in post["comments"]:
                    comment_id = str(uuid.uuid4())  # 댓글별 고유 ID 생성
                    comment_data.append(
                        (
                            comment_id,
                            post_id,
                            comment["comment_nickname"],
                            comment["comment_content"],
                            comment["comment_date"],
                            comment["comment_like_count"],
                            comment["comment_dislike_count"],
                        )
                    )

        # 컬럼 정의
        post_columns = [
            "post_id",
            "car_name",
            "source",
            "title",
            "author",
            "article",
            "timestamp",
            "view_cnt",
            "like_cnt",
            "dislike_cnt",
            "comment_count",
        ]
        comment_columns = [
            "comment_id",
            "post_id",
            "author",
            "content",
            "timestamp",
            "like_cnt",
            "dislike_cnt",
        ]

        # Spark DataFrame 생성 (Row 사용 가능)
        if post_data:
            new_post_df = spark.createDataFrame(post_data, post_columns)
        else:
            new_post_df = spark.createDataFrame([], post_columns)  # 빈 DataFrame 반환

        if comment_data:
            new_comments_df = spark.createDataFrame(comment_data, comment_columns)
        else:
            new_comments_df = spark.createDataFrame(
                [], comment_columns
            )  # 빈 DataFrame 반환

        return new_post_df, new_comments_df
    except Exception as e:
        print(f"[ERROR] {source} 데이터 변환 중 오류 발생: {e}")
        print(traceback.format_exc())
        return None, None


def make_sentence(df, type):
    try:
        if df is None or df.count() == 0:
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
        # df = df.withColumn("text", lower(col("text")))
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
        df = df.filter(col("text").rlike(".*[가-힣].*"))
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
    process_text(args.year, args.month, "그랜저")
