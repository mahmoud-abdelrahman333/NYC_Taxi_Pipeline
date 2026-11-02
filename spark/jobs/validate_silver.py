from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

spark = SparkSession.builder.appName("Validate Silver").getOrCreate()

conf = spark.sparkContext._jsc.hadoopConfiguration()
conf.set("fs.s3a.endpoint", "http://minio:9000")
conf.set("fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
conf.set("fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
conf.set("fs.s3a.path.style.access", "true")
conf.set("fs.s3a.connection.ssl.enabled", "false")

trips = spark.read.parquet(
    "s3a://silver/nyc_taxi/yellow_tripdata/year=2024/month=01/"
)

zones = spark.read.parquet(
    "s3a://silver/nyc_taxi/taxi_zones/"
)

print("\n===== COUNTS =====")
print("Trips:", trips.count())
print("Zones:", zones.count())

print("\n===== TRIPS SCHEMA =====")
trips.printSchema()

print("\n===== ZONES SCHEMA =====")
zones.printSchema()

print("\n===== NULL COUNTS =====")
columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "pulocationid",
    "dolocationid",
    "fare_amount",
    "total_amount",
]

trips.select([
    F.sum(F.col(c).isNull().cast("int")).alias(c)
    for c in columns
]).show()

print("\n===== UNMATCHED LOCATION IDS =====")
zone_ids = zones.select("locationid").distinct()

for column in ["pulocationid", "dolocationid"]:
    unmatched = (
        trips.select(F.col(column).alias("locationid"))
        .distinct()
        .join(zone_ids, "locationid", "left_anti")
    )

    print(column, unmatched.count())
    unmatched.show(20, truncate=False)

print("===== COUNTS =====")
print("Trips:", trips.count())
print("Zones:", zones.count())

print("===== PAYMENT TYPES =====")
trips.groupBy("payment_type").count().orderBy("payment_type").show()

print("===== RATE CODES =====")
trips.groupBy("ratecodeid").count().orderBy("ratecodeid").show()

print("===== VENDORS =====")
trips.groupBy("vendorid").count().orderBy("vendorid").show()

spark.stop()