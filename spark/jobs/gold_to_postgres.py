
import os

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("NYC-Taxi-Gold-to-Postgres")
    .getOrCreate()
)

# MinIO configuration
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", os.environ["MINIO_ROOT_USER"])
hadoop_conf.set("fs.s3a.secret.key", os.environ["MINIO_ROOT_PASSWORD"])
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# PostgreSQL connection
jdbc_url = (
    f"jdbc:postgresql://{os.environ['WAREHOUSE_HOST']}:"
    f"{os.environ['WAREHOUSE_PORT']}/"
    f"{os.environ['WAREHOUSE_DB']}"
)

jdbc_properties = {
    "user": os.environ["WAREHOUSE_USER"],
    "password": os.environ["WAREHOUSE_PASSWORD"],
    "driver": "org.postgresql.Driver"
}

GOLD_BASE_PATH = "s3a://gold/nyc_taxi"

# Read Gold dimensions and fact
dimensions = {
    "dim_date": f"{GOLD_BASE_PATH}/dimensions/dim_date/",
    "dim_location": f"{GOLD_BASE_PATH}/dimensions/dim_location/",
    "dim_payment": f"{GOLD_BASE_PATH}/dimensions/dim_payment/",
    "dim_rate": f"{GOLD_BASE_PATH}/dimensions/dim_rate/",
    "dim_vendor": f"{GOLD_BASE_PATH}/dimensions/dim_vendor/"
}

# Load dimensions first
for table_name, path in dimensions.items():
    print(f"Loading {table_name}...")

    df = spark.read.parquet(path)

    (
        df.write
        .mode("append")
        .jdbc(
            url=jdbc_url,
            table=table_name,
            properties=jdbc_properties
        )
    )

    print(f"{table_name} loaded: {df.count()} rows")

# Load fact table after dimensions
print("Loading fact_trips...")

fact_df = spark.read.parquet(
    f"{GOLD_BASE_PATH}/facts/fact_trips/"
)

(
    fact_df.write
    .mode("append")
    .jdbc(
        url=jdbc_url,
        table="fact_trips",
        properties=jdbc_properties
    )
)

print(f"fact_trips loaded: {fact_df.count()} rows")

print("Gold to PostgreSQL load completed!")

spark.stop()
