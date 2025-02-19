import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, split
from pyspark.sql.functions import lower, regexp_replace, trim, length
from pyspark.sql import functions as F

BUCKET_NAME = "the-all-new-bucket"


def seperate_post_and_comment(df, source, car_name):
    # `uuid()`로 `post_id` 생성
    post_df = df.withColumn("post_id", F.expr("uuid()"))

    # explode() 적용 전에 comment_count > 0 필터링
    comment_df = post_df.filter(F.col("comment_count") > 0)

    # explode() 적용
    comment_df = comment_df.withColumn("comment", F.explode("comments"))

    # rename columns
    comment_df = comment_df.select(
        F.expr("uuid()").alias("comment_id"),
        F.col("comment.comment_nickname").alias("author"),
        F.col("comment.comment_content").alias("content"),
        F.col("comment.comment_date").alias("timestamp"),
        F.col("comment.comment_like_count").alias("like_cnt"),
        F.col("comment.comment_dislike_count").alias("dislike_cnt"),
        "post_id",
    )

    post_df = post_df.drop("comments")
    post_df = post_df.withColumn("car_name", F.lit(car_name))
    post_df = post_df.withColumn("source", F.lit(source))

    # rename columns
    post_df = post_df.select(
        "post_id",
        "title",
        F.col("nickname").alias("author"),
        "article",
        F.col("date").alias("timestamp"),
        F.col("like_count").alias("like_cnt"),
        F.col("dislike_count").alias("dislike_cnt"),
        F.col("view_count").alias("view_cnt"),
        F.col("comment_count").alias("comment_cnt"),
        F.col("id").alias("source_post_id"),
    )
    return post_df, comment_df


def explode_post(post_df):
    title_sentence_df = post_df.select(
        F.expr("uuid()").alias("sentence_id"),
        col("post_id"),
        lit(None).cast("string").alias("comment_id"),
        lit("post").alias("type"),
        col("title").alias("text"),
    )
    article_sentence_df = post_df.select(
        F.expr("uuid()").alias("sentence_id"),
        col("post_id"),
        lit(None).alias("comment_id"),
        lit("post").alias("type"),
        explode(split(col("article"), "\n")).alias("text"),
    )

    sentence_df = title_sentence_df.union(article_sentence_df)

    return sentence_df


def explode_comment(comment_df):
    sentence_df = comment_df.select(
        F.expr("uuid()").alias("sentence_id"),
        col("post_id"),
        col("comment_id"),
        lit("comment").alias("type"),
        col("content").alias("text"),
    )
    return sentence_df


def clean_text(df):
    df = df.filter(length(col("text")) > 10)
    df = df.filter(col("text").rlike(".*[가-힣].*"))
    df = df.withColumn("text", lower(col("text")))
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


def process_text(year, month, day, car_name):
    spark = SparkSession.builder.appName("Process Text").getOrCreate()

    # s3에서 json 파일 읽어오기
    youtube_raw_df = spark.read.json(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/youtube_raw_*.json",
        multiLine=True,
    )
    bobae_raw_df = spark.read.json(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/bobae_raw.json",
        multiLine=True,
    )
    clien_raw_df = spark.read.json(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/clien_raw.json",
        multiLine=True,
    )

    # raw 데이터를 post, comment로 분리하기
    youtube_post_df, youtube_comment_df = seperate_post_and_comment(
        youtube_raw_df, "youtube", car_name
    )
    bobae_post_df, bobae_comment_df = seperate_post_and_comment(
        bobae_raw_df, "bobae", car_name
    )
    clien_post_df, clien_comment_df = seperate_post_and_comment(
        clien_raw_df, "clien", car_name
    )

    # source 별 post, comment 합치기
    post_df = youtube_post_df.union(bobae_post_df).union(clien_post_df)
    comment_df = youtube_comment_df.union(bobae_comment_df).union(clien_comment_df)

    # parquet 저장
    post_df.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/post_data"
    )
    comment_df.write.mode("overwrite").parquet(
        f"s3://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/comment_data"
    )

    # sentence 추출
    post_sentence_df = explode_post(post_df)
    comment_sentence_df = explode_comment(comment_df)

    sentence_df = post_sentence_df.union(comment_sentence_df)

    # sentence data를 rule based 로 정제(특수 문자 제거 등)
    sentence_df = clean_text(sentence_df)

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
