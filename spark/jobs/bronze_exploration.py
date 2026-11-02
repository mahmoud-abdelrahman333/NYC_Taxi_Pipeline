
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import os

# ==========================================
# 1. Spark Session
# ==========================================

spark = (
    SparkSession.builder
    .appName("NYC Taxi Bronze Exploration")
    .getOrCreate()
)


hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
hadoop_conf.set("fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# ==========================================
# 2. Read Bronze Data
# ==========================================

trips_path = (
    "s3a://bronze/nyc_taxi/yellow_tripdata/"
    "year=2024/month=01/yellow_tripdata_2024-01.parquet"
)

zones_path = "s3a://bronze/nyc_taxi/taxi_zones/taxi_zone_lookup.csv"

trips_df = spark.read.parquet(trips_path)

zones_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(zones_path)
)

# ==========================================
# 3. Data Understanding
# ==========================================

def explore_dataframe(df, name):
    print(f"\n{'=' * 50}")
    print(f"DATAFRAME: {name}")
    print("=" * 50)

    print("\nSchema:")
    df.printSchema()

    print("\nFirst 10 Rows:")
    df.show(10, truncate=False)

    print("\nTotal Rows:", df.count())
    print("Total Columns:", len(df.columns))

    print("\nColumn Names:")
    print(df.columns)

    print("\nData Types:")
    for column, data_type in df.dtypes:
        print(f"{column}: {data_type}")


explore_dataframe(trips_df, "Yellow Taxi Trips")
explore_dataframe(zones_df, "Taxi Zones")

# ==========================================
# 4. Missing Values Analysis
# ==========================================

def missing_values_analysis(df, name):
    print(f"\n{'=' * 50}")
    print(f"MISSING VALUES: {name}")
    print("=" * 50)

    total_rows = df.count()

    null_counts = df.agg(
        *[
            F.sum(
    F.when(F.col(c).isNull(), 1).otherwise(0)
          ).alias(c)
            for c in df.columns
        ]
    ).first().asDict()

    for column, null_count in null_counts.items():
        percentage = (
            null_count / total_rows * 100
            if total_rows > 0 else 0
        )

        print(
            f"{column}: {null_count} nulls "
            f"({percentage:.2f}%)"
        )


missing_values_analysis(trips_df, "Trips")
missing_values_analysis(zones_df, "Zones")

# ==========================================
# 5. Duplicate Analysis
# ==========================================

def duplicate_analysis(df, name):
    total_rows = df.count()
    distinct_rows = df.distinct().count()
    duplicates = total_rows - distinct_rows

    print(f"\n{name} Duplicate Rows: {duplicates}")


duplicate_analysis(trips_df, "Trips")
duplicate_analysis(zones_df, "Zones")

# ==========================================
# 6. Invalid Values Analysis
# ==========================================

invalid_checks = {
    "Non-positive trip distance":
        F.col("trip_distance") <= 0,

    "Negative fare amount":
        F.col("fare_amount") < 0,

    "Negative total amount":
        F.col("total_amount") < 0,

    "Non-positive passenger count":
        F.col("passenger_count") <= 0,

    "Dropoff before pickup":
        F.col("tpep_dropoff_datetime")
        < F.col("tpep_pickup_datetime"),

    "Same pickup and dropoff location":
        F.col("PULocationID") == F.col("DOLocationID")
}

print("\n" + "=" * 50)
print("INVALID VALUES ANALYSIS")
print("=" * 50)

for check_name, condition in invalid_checks.items():
    count = trips_df.filter(condition).count()
    print(f"{check_name}: {count}")

# ==========================================
# 7. Date & Time Analysis
# ==========================================

print("\n" + "=" * 50)
print("DATE & TIME ANALYSIS")
print("=" * 50)

trips_df.select(
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
    F.min("tpep_dropoff_datetime").alias("min_dropoff"),
    F.max("tpep_dropoff_datetime").alias("max_dropoff")
).show(truncate=False)

outside_january = trips_df.filter(
    (F.col("tpep_pickup_datetime") < F.lit("2024-01-01")) |
    (F.col("tpep_pickup_datetime") >= F.lit("2024-02-01"))
)

print("Pickup dates outside January 2024:", outside_january.count())

# ==========================================
# 8. Numerical Statistics
# ==========================================

numeric_columns = [
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "total_amount"
]

print("\n" + "=" * 50)
print("NUMERICAL STATISTICS")
print("=" * 50)

trips_df.select(
    *[
        F.min(c).alias(f"{c}_min")
        for c in numeric_columns
    ],
    *[
        F.max(c).alias(f"{c}_max")
        for c in numeric_columns
    ],
    *[
        F.avg(c).alias(f"{c}_avg")
        for c in numeric_columns
    ],
    *[
        F.stddev(c).alias(f"{c}_stddev")
        for c in numeric_columns
    ]
).show(truncate=False)

# ==========================================
# 9. Relationship Analysis
# ==========================================

print("\n" + "=" * 50)
print("RELATIONSHIP ANALYSIS")
print("=" * 50)

# Check uniqueness of LocationID

total_zones = zones_df.count()
distinct_location_ids = zones_df.select(
    "LocationID"
).distinct().count()

print("Total Zone Rows:", total_zones)
print("Distinct Location IDs:", distinct_location_ids)
print(
    "Duplicate Location IDs:",
    total_zones - distinct_location_ids
)

print(
    "Null Location IDs:",
    zones_df.filter(
        F.col("LocationID").isNull()
    ).count()
)

# Check unmatched Pickup IDs

zone_ids = zones_df.select(
    F.col("LocationID").alias("zone_id")
).distinct()

pickup_ids = trips_df.select(
    F.col("PULocationID").alias("location_id")
).distinct()

unmatched_pickup = pickup_ids.join(
    zone_ids,
    pickup_ids.location_id == zone_ids.zone_id,
    "left_anti"
)

print(
    "Unmatched Pickup IDs:",
    unmatched_pickup.count()
)

unmatched_pickup.show(truncate=False)

# Check unmatched Dropoff IDs

dropoff_ids = trips_df.select(
    F.col("DOLocationID").alias("location_id")
).distinct()

unmatched_dropoff = dropoff_ids.join(
    zone_ids,
    dropoff_ids.location_id == zone_ids.zone_id,
    "left_anti"
)

print(
    "Unmatched Dropoff IDs:",
    unmatched_dropoff.count()
)

unmatched_dropoff.show(truncate=False)

# ==========================================
# 10. Stop Spark
# ==========================================
print("\nStopping Spark Session...")
spark.stop()
