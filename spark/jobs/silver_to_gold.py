import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# Spark session
spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Silver-to-Gold")
    .getOrCreate()
)

# MinIO configuration
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


# Silver paths
TRIPS_PATH = (
    "s3a://silver/nyc_taxi/yellow_tripdata/"
    "year=2024/month=01/"
)

ZONES_PATH = "s3a://silver/nyc_taxi/taxi_zones/"


# Read Silver data
trips = spark.read.parquet(TRIPS_PATH)
zones = spark.read.parquet(ZONES_PATH)

print("===== DATE RANGE =====")

trips.select(
    F.min("tpep_pickup_datetime").alias("min_pickup"),
    F.max("tpep_pickup_datetime").alias("max_pickup"),
    F.min("tpep_dropoff_datetime").alias("min_dropoff"),
    F.max("tpep_dropoff_datetime").alias("max_dropoff")
).show(truncate=False)

print("===== OUTSIDE JANUARY 2024 =====")

outside_dates = trips.filter(
    (F.to_date("tpep_pickup_datetime") < F.lit("2024-01-01")) |
    (F.to_date("tpep_pickup_datetime") >= F.lit("2024-02-01")) |
    (F.to_date("tpep_dropoff_datetime") < F.lit("2024-01-01")) |
    (F.to_date("tpep_dropoff_datetime") >= F.lit("2024-02-01"))
)

print("Outside dates count:", outside_dates.count())

outside_dates.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(20, truncate=False)

suspicious_dates = trips.filter(
    (F.year("tpep_pickup_datetime") < 2023) |
    (F.year("tpep_dropoff_datetime") < 2023)
)

print("Suspicious old dates count:", suspicious_dates.count())

suspicious_dates.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "trip_distance",
    "fare_amount",
    "total_amount"
).show(30, truncate=False)

print("Trips:", trips.count())
print("Zones:", zones.count())

trips.printSchema()

# --------------------------------
# 1. Date Dimension
# --------------------------------

pickup_dates = trips.select(
    F.to_date("tpep_pickup_datetime").alias("full_date")
)

dropoff_dates = trips.select(
    F.to_date("tpep_dropoff_datetime").alias("full_date")
)

all_dates = (
    pickup_dates
    .union(dropoff_dates)
    .distinct()
    .filter(F.col("full_date").isNotNull())
)

dim_date = (
    all_dates
    .withColumn(
        "date_key",
        F.date_format("full_date", "yyyyMMdd").cast("int")
    )
    .withColumn("year", F.year("full_date"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day", F.dayofmonth("full_date"))
    .withColumn("day_name", F.date_format("full_date", "EEEE"))
    .select(
        "date_key",
        "full_date",
        "year",
        "quarter",
        "month",
        "day",
        "day_name"
    )
)

print("Date Dimension:", dim_date.count())

# --------------------------------
# 2. Location Dimension
# --------------------------------

location_window = Window.orderBy("locationid")

dim_location = (
    zones
    .withColumn(
        "location_key",
        F.row_number().over(location_window)
    )
    .select(
        "location_key",
        F.col("locationid"),
        "borough",
        "zone",
        "service_zone"
    )
)

print("Location Dimension:", dim_location.count())
# --------------------------------
# 3. Payment Dimension
# --------------------------------

payment_window = Window.orderBy("payment_type")

dim_payment = (
    trips
    .select("payment_type")
    .distinct()
    .withColumn(
        "payment_key",
        F.row_number().over(payment_window)
    )
    .withColumn(
        "payment_description",
        F.when(F.col("payment_type") == 0, "Flex Fare")
         .when(F.col("payment_type") == 1, "Credit Card")
         .when(F.col("payment_type") == 2, "Cash")
         .when(F.col("payment_type") == 3, "No Charge")
         .when(F.col("payment_type") == 4, "Dispute")
         .when(F.col("payment_type") == 5, "Unknown")
         .when(F.col("payment_type") == 6, "Voided Trip")
         .otherwise("Unrecognized")
    )
    .select(
        "payment_key",
        "payment_type",
        "payment_description"
    )
)


# --------------------------------
# 4. Rate Dimension
# --------------------------------

rate_window = Window.orderBy("ratecodeid")

dim_rate = (
    trips
    .select("ratecodeid")
    .distinct()
    .withColumn(
        "rate_key",
        F.row_number().over(rate_window)
    )
    .withColumn(
        "rate_description",
        F.when(F.col("ratecodeid") == 1, "Standard Rate")
         .when(F.col("ratecodeid") == 2, "JFK")
         .when(F.col("ratecodeid") == 3, "Newark")
         .when(F.col("ratecodeid") == 4, "Nassau or Westchester")
         .when(F.col("ratecodeid") == 5, "Negotiated Fare")
         .when(F.col("ratecodeid") == 6, "Group Ride")
         .when(F.col("ratecodeid") == 99, "Null or Unknown")
         .otherwise("Unrecognized")
    )
    .select(
        "rate_key",
        "ratecodeid",
        "rate_description"
    )
)

# --------------------------------
# 5. Vendor Dimension
# --------------------------------

vendor_window = Window.orderBy("vendorid")

dim_vendor = (
    trips
    .select("vendorid")
    .distinct()
    .withColumn(
        "vendor_key",
        F.row_number().over(vendor_window)
    )
    .withColumn(
        "vendor_name",
        F.when(
            F.col("vendorid") == 1,
            "Creative Mobile Technologies"
        )
        .when(
            F.col("vendorid") == 2,
            "Curb Mobility"
        )
        .otherwise("Unrecognized Vendor")
    )
    .select(
        "vendor_key",
        "vendorid",
        "vendor_name"
    )
)
print("\n===== DATE =====")
dim_date.orderBy("date_key").show(10, truncate=False)

print("\n===== LOCATION =====")
dim_location.orderBy("location_key").show(10, truncate=False)

print("\n===== PAYMENT =====")
dim_payment.orderBy("payment_key").show(truncate=False)

print("\n===== RATE =====")
dim_rate.orderBy("rate_key").show(truncate=False)

print("\n===== VENDOR =====")
dim_vendor.orderBy("vendor_key").show(truncate=False)


# --------------------------------
# 6. Fact Table
# --------------------------------

# Aliases for dimensions
t = trips.alias("t")

pickup_date = dim_date.alias("pickup_date")
dropoff_date = dim_date.alias("dropoff_date")

pickup_location = dim_location.alias("pickup_location")
dropoff_location = dim_location.alias("dropoff_location")

payment = dim_payment.alias("payment")
rate = dim_rate.alias("rate")
vendor = dim_vendor.alias("vendor")


# Join trips with all dimensions
fact_trips = (
    t

    # Pickup Date
    .join(
        pickup_date,
        F.to_date(F.col("t.tpep_pickup_datetime")) ==
        F.col("pickup_date.full_date"),
        "left"
    )

    # Dropoff Date
    .join(
        dropoff_date,
        F.to_date(F.col("t.tpep_dropoff_datetime")) ==
        F.col("dropoff_date.full_date"),
        "left"
    )

    # Pickup Location
    .join(
        pickup_location,
        F.col("t.PULocationID") ==
        F.col("pickup_location.locationid"),
        "left"
    )

    # Dropoff Location
    .join(
        dropoff_location,
        F.col("t.DOLocationID") ==
        F.col("dropoff_location.locationid"),
        "left"
    )

    # Payment
    .join(
        payment,
        F.col("t.payment_type").eqNullSafe(
            F.col("payment.payment_type")
        ),
        "left"
    )

    # Rate
    .join(
        rate,
        F.col("t.ratecodeid").eqNullSafe(
            F.col("rate.ratecodeid")
        ),
        "left"
    )

    # Vendor
    .join(
        vendor,
        F.col("t.vendorid").eqNullSafe(
            F.col("vendor.vendorid")
        ),
        "left"
    )

    # Select fact columns
    .select(
        # Deterministic trip key
        F.sha2(
            F.to_json(
                F.struct(*[
                    F.col(f"t.{c}") for c in trips.columns
                ])
            ),
            256
        ).alias("trip_key"),

        # Date keys
        F.col("pickup_date.date_key").alias("pickup_date_key"),
        F.col("dropoff_date.date_key").alias("dropoff_date_key"),

        # Location keys
        F.col("pickup_location.location_key").alias("pickup_location_key"),
        F.col("dropoff_location.location_key").alias("dropoff_location_key"),

        # Other dimension keys
        F.col("payment.payment_key").alias("payment_key"),
        F.col("rate.rate_key").alias("rate_key"),
        F.col("vendor.vendor_key").alias("vendor_key"),

        # Timestamps
        F.col("t.tpep_pickup_datetime").alias("pickup_datetime"),
        F.col("t.tpep_dropoff_datetime").alias("dropoff_datetime"),

        # Measures
        F.col("t.passenger_count"),
        F.col("t.trip_distance"),
        F.col("t.fare_amount"),
        F.col("t.extra"),
        F.col("t.mta_tax"),
        F.col("t.tip_amount"),
        F.col("t.tolls_amount"),
        F.col("t.improvement_surcharge"),
        F.col("t.total_amount"),
        F.col("t.congestion_surcharge"),
        F.col("t.airport_fee"),

        # Degenerate dimension attribute
        F.col("t.store_and_fwd_flag")
    )
)

print("\n===== FACT TRIPS =====")
print("Fact Trips Count:", fact_trips.count())

fact_trips.printSchema()
fact_trips.show(10, truncate=False)

# Check missing dimension keys
print("\n===== NULL FOREIGN KEYS =====")

fact_trips.select(
    *[
        F.sum(
            F.when(F.col(c).isNull(), 1).otherwise(0)
        ).alias(c)
        for c in [
            "pickup_date_key",
            "dropoff_date_key",
            "pickup_location_key",
            "dropoff_location_key",
            "payment_key",
            "rate_key",
            "vendor_key"
        ]
    ]
).show()


print("Silver trips:", trips.count())
print("Fact trips:", fact_trips.count())
print("Unique trip keys:", fact_trips.select("trip_key").distinct().count())



# --------------------------------
# 7. Write Gold Data to MinIO
# --------------------------------


GOLD_BASE_PATH = "s3a://gold/nyc_taxi"

# Write dimensions
dim_date.write.mode("overwrite").parquet(
    f"{GOLD_BASE_PATH}/dimensions/dim_date/"
)

dim_location.write.mode("overwrite").parquet(
    f"{GOLD_BASE_PATH}/dimensions/dim_location/"
)

dim_payment.write.mode("overwrite").parquet(
    f"{GOLD_BASE_PATH}/dimensions/dim_payment/"
)

dim_rate.write.mode("overwrite").parquet(
    f"{GOLD_BASE_PATH}/dimensions/dim_rate/"
)

dim_vendor.write.mode("overwrite").parquet(
    f"{GOLD_BASE_PATH}/dimensions/dim_vendor/"
)

# Write fact table
fact_trips.write.mode("overwrite").parquet(
    f"{GOLD_BASE_PATH}/facts/fact_trips/"
)

print("Gold data successfully written to MinIO!")
