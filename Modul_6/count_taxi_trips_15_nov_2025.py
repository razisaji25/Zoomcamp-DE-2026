from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date

spark = SparkSession.builder \
    .appName("question3") \
    .getOrCreate()

df = spark.read.parquet("yellow_tripdata_2025-11.parquet")

# filter pickup tanggal 15 November
df_filtered = df.filter(
    to_date(col("tpep_pickup_datetime")) == "2025-11-15"
)

print(df_filtered.count())

spark.stop()