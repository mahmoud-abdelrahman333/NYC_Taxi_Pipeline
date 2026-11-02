
from datetime import datetime
from pathlib import Path
from urllib.request import urlretrieve
import os
import tempfile
import boto3
from airflow.sdk import dag, task


@dag(
    dag_id="nyc_taxi_ingestion",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "ingestion"],
)
def nyc_taxi_ingestion():

    @task
    def download_to_bronze(url: str, object_key: str):
        s3 = boto3.client(
            "s3",
            endpoint_url="http://minio:9000",
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
            region_name="us-east-1",
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            filename = url.split("/")[-1]
            local_path = Path(temp_dir) / filename

            urlretrieve(url, local_path)

            s3.upload_file(
                str(local_path),
                "bronze",
                object_key,
            )

        return object_key

    base_url = "https://d37ci6vzurychx.cloudfront.net"

    trips = download_to_bronze(
        url=f"{base_url}/trip-data/yellow_tripdata_2024-01.parquet",
        object_key="nyc_taxi/yellow_tripdata/year=2024/month=01/yellow_tripdata_2024-01.parquet",
    )

    zones = download_to_bronze(
        url=f"{base_url}/misc/taxi_zone_lookup.csv",
        object_key="nyc_taxi/taxi_zones/taxi_zone_lookup.csv",
    )


nyc_taxi_ingestion()
