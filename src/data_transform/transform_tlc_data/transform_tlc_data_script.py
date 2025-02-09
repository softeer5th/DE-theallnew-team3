from pyspark.sql import SparkSession
from pyspark.sql.functions import count, sum, avg

spark = SparkSession.builder.appName("TLC Data Transform").getOrCreate()

bucket = 'simple-s3-jy'
input_key = "yellow_tripdata_2024-01.parquet_cleaned/"
output_key = "yellow_tripdata_2024-01.parquet_transform"

df = spark.read.parquet(f"s3://{bucket}/{input_key}")


total_revenue = df.select(sum(df.total_amount)).collect()[0][0]
average_trip_distance = df.select(avg(df.trip_distance)).collect()[0][0]

trips_per_day_df = df.groupBy("date").agg(count("*").alias("total_trips"))
df = df.join(trips_per_day_df, on="date", how="left")

total_revenue_per_day = df.groupBy("date").agg(
    sum("total_amount").alias("total_revenue")
)
df = df.join(total_revenue_per_day, on="date", how="left")

df.write.parquet(f"s3://{bucket}/{output_key}")