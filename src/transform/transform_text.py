import json

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, split
from pyspark.sql.functions import col, lower, regexp_replace, trim, length

SOURCE_TYPE = "youtube"

def to_flattend(df):
    title_df = df.select(
        col("title").alias("text"),
        col("date"),
        col("like_count"),
        col("view_count"),
        lit(SOURCE_TYPE).alias("source"),
    )

    article_df = df.select(
        explode(split(col("article"), "\n")).alias("text"),
        col("date"),
        col("like_count"),
        col("view_count"),
        lit(SOURCE_TYPE).alias("source"),
    )

    comment_df = df.select(explode(col("comments")).alias("comment")).select(
        col("comment.comment_content").alias("text"),
        col("comment.comment_date").alias("date"),
        col("comment.comment_like_count").alias("like_count"),
        lit(None).alias("view_count"),
        lit(SOURCE_TYPE).alias("source"),
    )

    flattened_df = title_df.union(article_df).union(comment_df)
    return flattened_df

def to_cleaned(df):
    df = df.withColumn("text", lower(col("text")))

    df = df.withColumn("text", regexp_replace(col("text"), r"http\S+", ""))
    df = df.withColumn("text", regexp_replace(col("text"), r"https\S+", ""))
    df = df.withColumn("text", regexp_replace(col("text"), r"@\S+", ""))
    df = df.withColumn("text", regexp_replace(col("text"), r"#\S+", ""))

    df = df.withColumn("text", regexp_replace(col("text"), r"([.!?,~])\1+", "$1"))

    df = df.withColumn("text", regexp_replace(col("text"), r"[\u200B-\u200F\u202A-\u202E\u2060-\u206F\uFEFF\u00A0]", ""))
    df = df.withColumn("text", trim(regexp_replace(col("text"), r"\s+", " ")))

    df = df.withColumn("text", regexp_replace(col("text"), r"투산+", "투싼"))
    df = df.filter(col("text").rlike(".*[가-힣].*"))

    df = df.filter(length(col("text")) > 10)
    return df

def unify_staging_data(input_json):
    sources = ['youtube', 'bobae', 'clien']

    unified_data = []
    for source in sources:
        source_data_file = f'{source}_{input_json}'

        with open(f"data/{source_data_file}", "r", encoding="utf-8") as f:
            source_data = json.load(f)
        unified_data.extend(source_data)

    with open(f"data/{input_json}", "w", encoding="utf-8") as f:
        json.dump(unified_data, f, ensure_ascii=False, indent=4)

def transform_text(input_date, car_name):
    spark = (
        SparkSession.builder.appName("Trasnform")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )

    input_json = f'{input_date}_{car_name}.json'
    unify_staging_data(input_json)

    df = spark.read.json(f"data/{input_json}", multiLine=True)

    df = to_flattend(df)
    df = to_cleaned(df)

    df.write.csv("data/transformed", header=True, mode="overwrite")

    spark.stop()