
from datetime import datetime

from airflow.sdk import dag
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


@dag(
    dag_id="nyc_taxi_parent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["nyc-taxi", "orchestration"],
)
def nyc_taxi_parent():

    ingestion = TriggerDagRunOperator(
        task_id="run_ingestion",
        trigger_dag_id="nyc_taxi_ingestion",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    bronze_to_silver = TriggerDagRunOperator(
        task_id="run_bronze_to_silver",
        trigger_dag_id="nyc_taxi_bronze_to_silver",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    silver_to_gold = TriggerDagRunOperator(
        task_id="run_silver_to_gold",
        trigger_dag_id="nyc_taxi_silver_to_gold",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    gold_to_postgres = TriggerDagRunOperator(
        task_id="run_gold_to_postgres",
        trigger_dag_id="nyc_taxi_gold_to_postgres",
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    ingestion >> bronze_to_silver >> silver_to_gold >> gold_to_postgres


nyc_taxi_parent()
