
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="nyc_taxi_bronze_to_silver",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "spark", "silver"],
)
def nyc_taxi_bronze_to_silver():

    bronze_to_silver = BashOperator(
        task_id="run_bronze_to_silver",
        bash_command="""
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/opt/spark-ivy \
            --packages org.apache.hadoop:hadoop-aws:3.5.0 \
            /opt/spark-jobs/bronze_to_silver.py
        """,
    )


nyc_taxi_bronze_to_silver()
