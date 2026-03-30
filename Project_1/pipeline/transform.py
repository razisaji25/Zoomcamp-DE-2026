from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def run(year):
    spark = SparkSession.builder.appName("CRSS Pipeline").getOrCreate()

    base_path = f"data/{year}"

    accident = spark.read.csv(f"{base_path}/accident.csv", header=True, inferSchema=True)
    vehicle  = spark.read.csv(f"{base_path}/vehicle.csv", header=True, inferSchema=True)

    accident = accident.alias("a")
    vehicle  = vehicle.alias("v")

    df = accident.join(vehicle, "ST_CASE", "left")

    df_final = df.select(
        col("a.ST_CASE").alias("case_id"),
        col("a.YEAR").alias("year"),
        col("a.MONTH").alias("month"),
        col("a.HOUR").alias("hour"),
        col("a.WEATHERNAME").alias("weather"),
        col("v.HARM_EVNAME").alias("harm_event"),
        col("v.MAN_COLLNAME").alias("collision_type")
    )

    output_path = f"data/processed/year={year}"
    df_final.write.mode("overwrite").parquet(output_path)

if __name__ == "__main__":
    base_dir = "data"

    years = [d for d in os.listdir(base_dir) if d.isdigit()]

    for year in years:
        print(f"Processing year: {year}")
        run(year)