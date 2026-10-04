from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


default_args = {
    "owner": "youssef",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="ocp_transport_batch_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ocp", "transport", "batch", "data-engineering"],
) as dag:

    load_postgres_to_snowflake = BashOperator(
        task_id="load_postgres_to_snowflake",
        bash_command="python /opt/airflow/project/src/load_postgres_to_snowflake.py",
    )

    bronze_ingestion = BashOperator(
        task_id="bronze_ingestion",
        bash_command="python /opt/airflow/project/src/pyspark/bronze_ingestion.py",
    )

    silver_transformation = BashOperator(
        task_id="silver_transformation",
        bash_command="python /opt/airflow/project/src/pyspark/silver_transformation.py",
    )

    gold_vehicle_kpis = BashOperator(
        task_id="gold_vehicle_kpis",
        bash_command="python /opt/airflow/project/src/pyspark/gold_vehicle_kpis.py",
    )

    gold_route_kpis = BashOperator(
        task_id="gold_route_kpis",
        bash_command="python /opt/airflow/project/src/pyspark/gold_route_kpis.py",
    )

    gold_driver_kpis = BashOperator(
        task_id="gold_driver_kpis",
        bash_command="python /opt/airflow/project/src/pyspark/gold_driver_kpis.py",
    )

    gold_fleet_kpis = BashOperator(
        task_id="gold_fleet_kpis",
        bash_command="python /opt/airflow/project/src/pyspark/gold_fleet_kpis.py",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/project/dbt/ocp_transport && dbt run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /opt/airflow/project/dbt/ocp_transport && dbt test",
    )

    load_postgres_to_snowflake >> bronze_ingestion
    bronze_ingestion >> silver_transformation

    silver_transformation >> [
        gold_vehicle_kpis,
        gold_route_kpis,
        gold_driver_kpis,
        gold_fleet_kpis,
    ]

    [
        gold_vehicle_kpis,
        gold_route_kpis,
        gold_driver_kpis,
        gold_fleet_kpis,
    ] >> dbt_run >> dbt_test