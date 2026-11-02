
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    DoubleType,
    TimestampType
)
import os

# ==========================================
# 1. Spark Session
# ==========================================

spark = (
    SparkSession.builder
    .appName("NYC Taxi Bronze to Silver")
    .getOrCreate()
)

# ==========================================
# 2. MinIO Configuration
# ==========================================

hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set(
    "fs.s3a.access.key",
    os.environ["MINIO_ROOT_USER"]
)
hadoop_conf.set(
    "fs.s3a.secret.key",
    os.environ["MINIO_ROOT_PASSWORD"]
)
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# ==========================================
# 3. Source and Destination Paths
# ==========================================

trips_bronze = (
    "s3a://bronze/nyc_taxi/yellow_tripdata/"
    "year=2024/month=01/yellow_tripdata_2024-01.parquet"
)

zones_bronze = (
    "s3a://bronze/nyc_taxi/taxi_zones/"
    "taxi_zone_lookup.csv"
)

trips_silver = (
    "s3a://silver/nyc_taxi/yellow_tripdata/"
    "year=2024/month=01/"
)

zones_silver = (
    "s3a://silver/nyc_taxi/taxi_zones/"
)

# ==========================================
# 4. Read Bronze Data
# ==========================================

trips_df = spark.read.parquet(trips_bronze)

zones_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(zones_bronze)
)

print("Bronze Trips:", trips_df.count())
print("Bronze Zones:", zones_df.count())

# ==========================================
# 5. Clean Trips
# ==========================================

# Standardize column names
trips_df = trips_df.toDF(
    *[column.lower() for column in trips_df.columns]
)

# Remove rows with missing critical fields
trips_df = trips_df.dropna(
    subset=[
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "pulocationid",
        "dolocationid",
        "fare_amount",
        "total_amount"
    ]
)

# Cast columns to appropriate data types
trips_df = (
    trips_df
    .withColumn("vendorid", F.col("vendorid").cast(IntegerType()))
    .withColumn("passenger_count", F.col("passenger_count").cast(IntegerType()))
    .withColumn("trip_distance", F.col("trip_distance").cast(DoubleType()))
    .withColumn("ratecodeid", F.col("ratecodeid").cast(IntegerType()))
    .withColumn("pulocationid", F.col("pulocationid").cast(IntegerType()))
    .withColumn("dolocationid", F.col("dolocationid").cast(IntegerType()))
    .withColumn("payment_type", F.col("payment_type").cast(IntegerType()))
    .withColumn("fare_amount", F.col("fare_amount").cast(DoubleType()))
    .withColumn("tip_amount", F.col("tip_amount").cast(DoubleType()))
    .withColumn("total_amount", F.col("total_amount").cast(DoubleType()))
    .withColumn("tpep_pickup_datetime", F.col("tpep_pickup_datetime").cast(TimestampType()))
    .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
)

# Remove invalid records
trips_df = trips_df.filter(
    (F.col("trip_distance") > 0) &
    (F.col("fare_amount") >= 0) &
    (F.col("total_amount") >= 0) &
    (F.col("passenger_count") > 0) &
    (F.col("tpep_dropoff_datetime") >= F.col("tpep_pickup_datetime")) &
    (F.col("tpep_pickup_datetime") >= F.lit("2024-01-01")) &
    (F.col("tpep_pickup_datetime") < F.lit("2024-02-01"))
)

# Remove exact duplicate rows
trips_df = trips_df.dropDuplicates()

# ==========================================
# 6. Clean Taxi Zones
# ==========================================

zones_df = zones_df.toDF(
    *[column.lower() for column in zones_df.columns]
)

zones_df = (
    zones_df
    .withColumn("locationid", F.col("locationid").cast(IntegerType()))
    .withColumn("borough", F.trim(F.col("borough")))
    .withColumn("zone", F.trim(F.col("zone")))
    .withColumn("service_zone", F.trim(F.col("service_zone")))
)

zones_df = zones_df.dropna(
    subset=["locationid", "borough", "zone"]
)

zones_df = zones_df.dropDuplicates(["locationid"])

# ==========================================
# 7. Data Quality Checks
# ==========================================

print("Silver Trips:", trips_df.count())
print("Silver Zones:", zones_df.count())

print("Trips with NULL Pickup IDs:",
      trips_df.filter(F.col("pulocationid").isNull()).count())

print("Trips with NULL Dropoff IDs:",
      trips_df.filter(F.col("dolocationid").isNull()).count())

print("Duplicate Zone IDs:",
      zones_df.count() - zones_df.select("locationid").distinct().count())

# ==========================================
# 8. Write Silver Data
# ==========================================

trips_df.write \
    .mode("overwrite") \
    .parquet(trips_silver)

zones_df.write \
    .mode("overwrite") \
    .parquet(zones_silver)

print("Bronze to Silver completed successfully.")

# ==========================================
# 9. Stop Spark
# ==========================================

spark.stop()
