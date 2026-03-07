from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, max

spark = SparkSession.builder \
    .appName("longest_trip") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df_duration = df.withColumn(
    "trip_hours",
    (unix_timestamp(col("tpep_dropoff_datetime")) -
     unix_timestamp(col("tpep_pickup_datetime"))) / 3600
)

df_duration.select(max("trip_hours")).show()

spark.stop()