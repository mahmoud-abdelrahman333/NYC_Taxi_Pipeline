
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag(
    dag_id="nyc_taxi_gold_to_postgres",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "spark", "postgres"],
)
def nyc_taxi_gold_to_postgres():

    gold_to_postgres = BashOperator(
        task_id="run_gold_to_postgres",
        bash_command="""
        /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/opt/spark-ivy \
            --packages org.apache.hadoop:hadoop-aws:3.5.0,org.postgresql:postgresql:42.7.7 \
            /opt/spark-jobs/gold_to_postgres.py
        """,
    )


nyc_taxi_gold_to_postgres()
