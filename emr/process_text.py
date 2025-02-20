import argparse
from datetime import datetime, timedelta, timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, split, udf
from pyspark.sql.functions import lower, regexp_replace, trim, length
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

BUCKET_NAME = "the-all-new-bucket"

def get_timestamp(year,month,day):
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

def save_missing_post_data(df, data_type, car_name, year, month, day):
    """
    결측치가 있는 데이터를 별도로 저장하는 함수
    """
    # 결측치가 있는 데이터를 필터링 
    missing_data_df = df.filter(
        col("id").isNull() |
        col("car_name").isNull() |
        col("source").isNull() |
        col("title").isNull() | 
        col("nickname").isNull() | 
        col("article").isNull() |
        col("view_count").isNull() | (col("view_count") < 0) |
        col("like_count").isNull() | (col("like_count") < 0) |
        col("dislike_count").isNull() |  (col("dislike_count") < 0) |
        col("date").isNull() | 
        col("comment_count").isNull() | (col("comment_count") < 0) 
    )

    if missing_data_df.count() > 0:
        # 누락된 데이터가 있을 경우, 별도의 파일로 저장
        missing_data_df.write.mode("overwrite").parquet(
            f"s3a://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/missing_data/{data_type}_post_missing"
        )
        print(f"Missing data for {data_type} saved.")
    else:
        print(f"No missing data for {data_type}.")
    
    # 결측치가 있는 데이터를 제외한 DataFrame 반환
    df_cleaned = df.filter(
        col("id").isNotNull() &
        col("car_name").isNotNull() &
        col("source").isNotNull() &
        col("title").isNotNull() &
        col("nickname").isNotNull() & 
        col("article").isNotNull() &
        col("view_count").isNotNull() &
        col("like_count").isNotNull() &
        col("dislike_count").isNotNull() &
        col("date").isNotNull() &
        col("comment_count").isNotNull()
    )

    return df_cleaned

def save_missing_comment_data(df, data_type, car_name, year, month, day):
    """
    결측치가 있는 데이터를 별도로 저장하는 함수
    """
    # 결측치가 있는 데이터를 필터링 (timestamp, title 등에서 null 체크)
    missing_data_df = df.filter(
        col("comment_uuid").isNull() |
        col("author").isNull() |
        col("content").isNull() |
        col("timestamp").isNull() | 
        col("like_cnt").isNull() | (col("like_cnt") < 0) |
        col("dislike_cnt").isNull() |(col("dislike_cnt") < 0) 
    )

    if missing_data_df.count() > 0:
        # 누락된 데이터가 있을 경우, 별도의 파일로 저장
        missing_data_df.write.mode("overwrite").parquet(
            f"s3a://{BUCKET_NAME}/{car_name}/{year}/{month}/{day}/missing_data/{data_type}_comment_missing"
        )
        print(f"Missing data for {data_type} saved.")
    else:
        print(f"No missing data for {data_type}.")
    
    # 결측치가 있는 데이터를 제외한 DataFrame 반환
    df_cleaned = df.filter(
        col("comment_uuid").isNotNull() & 
        col("author").isNotNull() & 
        col("content").isNotNull() & 
        col("timestamp").isNotNull() & 
        col("like_cnt").isNotNull() & 
        col("dislike_cnt").isNotNull()
    )

    return df_cleaned



def seperate_post_and_comment(df, source, car_name,year,month,day):
    
    #post data의 결측치&이상치 따로 저장
    df = save_missing_post_data(df, source, car_name, year, month, day)
    
    # `uuid()`로 `post_uuid` 생성
    post_df = df.withColumn("post_uuid", F.expr("uuid()"))

    # explode() 적용 전에 comment_count > 0 필터링
    comment_df = post_df.filter(F.col("comment_count") > 0)

    # explode() 적용
    comment_df = comment_df.withColumn("comment", F.explode("comments"))

    # rename columns
    comment_df = comment_df.select(
        F.expr("uuid()").alias("comment_uuid"),
        F.col("comment.comment_nickname").alias("author"),
        F.col("comment.comment_content").alias("content"),
        F.col("comment.comment_date").alias("timestamp"),
        F.col("comment.comment_like_count").alias("like_cnt"),
        F.col("comment.comment_dislike_count").alias("dislike_cnt"),
        "post_uuid",
    )
    
    # comment data의 결측치&이상치 따로 저장
    comment_df = save_missing_comment_data(comment_df, source, car_name, year, month, day)
    
    post_df = post_df.drop("comments")
    #post_df = post_df.withColumn("car_name", F.lit(car_name))
    #post_df = post_df.withColumn("source", F.lit(source))

    # rename columns
    post_df = post_df.select(
        "post_uuid",
        F.col("id").alias("post_id"),
        "title",
        F.col("nickname").alias("author"),
        "article",
        F.col("date").alias("timestamp"),
        F.col("like_count").alias("like_cnt"),
        F.col("dislike_count").alias("dislike_cnt"),
        F.col("view_count").alias("view_cnt"),
        F.col("comment_count").alias("comment_cnt"),
        "source",
        "car_name"
    )
    return post_df, comment_df


def explode_post(post_df):
    title_sentence_df = post_df.select(
        F.expr("uuid()").alias("sentence_uuid"),
        col("post_uuid"),
        lit(None).cast("string").alias("comment_id"),
        lit("post").alias("type"),
        col("title").alias("text"),
    )
    article_sentence_df = post_df.select(
        F.expr("uuid()").alias("sentence_id"),
        col("post_uuid"),
        lit(None).alias("comment_uuid"),
        lit("post").alias("type"),
        explode(split(col("article"), "\n")).alias("text"),
    )

    sentence_df = title_sentence_df.union(article_sentence_df)

    return sentence_df


def explode_comment(comment_df):
    sentence_df = comment_df.select(
        F.expr("uuid()").alias("sentence_uuid"),
        col("post_uuid"),
        col("comment_uuid"),
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
        youtube_raw_df, "youtube", car_name, year,month,day
    )
    bobae_post_df, bobae_comment_df = seperate_post_and_comment(
        bobae_raw_df, "bobae", car_name,year,month,day
    )
    clien_post_df, clien_comment_df = seperate_post_and_comment(
        clien_raw_df, "clien", car_name,year,month,day
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

    # 새로 생긴 데이터만 남기기
    start_timestamp, end_timestamp = get_timestamp(year,month, day)
    
    comment_df = comment_df.filter(
        (start_timestamp<=col("timestamp"))& (col("timestamp")<=end_timestamp)
    )   
    post_df = post_df.filter(
        (start_timestamp<=col("timestamp"))& (col("timestamp")<=end_timestamp)
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
