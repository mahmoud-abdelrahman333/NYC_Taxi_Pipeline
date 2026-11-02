
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="nyc_taxi_silver_to_gold",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "spark", "gold"],
)
def nyc_taxi_silver_to_gold():

    silver_to_gold = BashOperator(
        task_id="run_silver_to_gold",
        bash_command="""
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/opt/spark-ivy \
            --packages org.apache.hadoop:hadoop-aws:3.5.0 \
            /opt/spark-jobs/silver_to_gold.py
        """,
    )


nyc_taxi_silver_to_gold()
