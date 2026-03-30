from pyspark.sql import SparkSession
import os

spark = SparkSession.builder \
    .appName("yellow_2025_11") \
    .getOrCreate()

# read parquet
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# repartition
df_4 = df.repartition(4)
uv run python yellow_2025_11_spark.py
# write parquet
output_path = "yellow_2025_11_repartitioned"
df_4.write.mode("overwrite").parquet(output_path)

spark.stop()