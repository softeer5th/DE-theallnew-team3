# create parquet file
# id: uuid
# text: random string size 10~100
# timestamp: 2023-01-01 00:00:00 ~ 2025-02-25 00:00:00
# like_cnt: random int 0~100
# view_cnt: random int 0~100
# source: "youtube": 0.8, "clien": 0.1, "bobae": 0.1

ROWS = 500_000_000

# pyspark


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand, date_add, floor

# Initialize Spark session
spark = (
    SparkSession.builder.appName("GenerateParquet")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .getOrCreate()
)

df = (
    spark.range(ROWS)
    .withColumn("id", expr("uuid()"))
    .withColumn("text", expr("substring(md5(uuid()), 1, floor(rand() * 91 + 10))"))
    .withColumn(
        "timestamp", date_add(expr("date('2023-01-01')"), (rand() * 782).cast("int"))
    )
    .withColumn("like_cnt", floor(rand() * 101))
    .withColumn("view_cnt", floor(rand() * 101))
    .withColumn(
        "source",
        expr(
            "case when rand() < 0.8 then 'youtube' "
            + "when rand() < 0.9 then 'clien' else 'bobae' end"
        ),
    )
)

# Save as Parquet file
output_path = "generated_data.parquet"
df.write.mode("overwrite").parquet(output_path)

print(f"Parquet file saved at: {output_path}")

# Stop Spark session
spark.stop()
