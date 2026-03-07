from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("zone_lookup") \
    .getOrCreate()

df_trips = spark.read.parquet("yellow_tripdata_2025-11.parquet")

df_zones = spark.read.option("header", "true") \
    .csv("taxi_zone_lookup.csv")

df_trips.createOrReplaceTempView("trips")
df_zones.createOrReplaceTempView("zones")

result = spark.sql("""
SELECT
    z.Zone,
    COUNT(*) as trips
FROM trips t
JOIN zones z
ON t.PULocationID = z.LocationID
GROUP BY z.Zone
ORDER BY trips ASC
LIMIT 1
""")

result.show()

spark.stop()