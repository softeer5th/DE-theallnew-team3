from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

spark = SparkSession.builder.appName("TLC Data Cleansing").getOrCreate()

bucket = 'simple-s3-jy'
key = "yellow_tripdata_2024-01.parquet"

df = spark.read.parquet(f"s3://{bucket}/{key}")

df = df.dropna(
    subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
    ]
)
df = df.filter(df.fare_amount > 0)
df = df.filter(df.trip_distance > 0)

# datetime to date
df = df.withColumn("date", to_date(df.tpep_pickup_datetime))
df = df.filter(df.date < "2024-02-01")
df = df.filter(df.date >= "2024-01-01")

df.write.parquet(f"s3://{bucket}/{key}_cleaned")