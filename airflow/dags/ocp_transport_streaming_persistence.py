from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

STREAMING_DIR = "/opt/airflow/project/src/streaming"
DBT_DIR = "/opt/airflow/project/dbt/ocp_transport"

default_args = {
    "owner": "youssef",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ocp_transport_streaming_persistence",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=timedelta(minutes=15),
    catchup=False,
    max_active_runs=1,
    tags=["ocp", "transport", "streaming", "persistence"],
) as dag:

    telemetry_to_minio = BashOperator(
        task_id="telemetry_to_minio",
        bash_command=f"python {STREAMING_DIR}/spark_telemetry_stream.py --available-now",
    )

    realtime_kpis_to_minio = BashOperator(
        task_id="realtime_kpis_to_minio",
        bash_command=f"python {STREAMING_DIR}/spark_realtime_kpis.py --available-now",
    )

    alerts_to_minio_and_kafka = BashOperator(
        task_id="alerts_to_minio_and_kafka",
        bash_command=f"python {STREAMING_DIR}/spark_alert_engine.py --available-now",
    )

    minio_to_snowflake_raw = BashOperator(
        task_id="minio_to_snowflake_raw",
        bash_command=f"python -W ignore {STREAMING_DIR}/load_streaming_to_snowflake.py",
    )

    dbt_run_streaming = BashOperator(
        task_id="dbt_run_streaming",
        bash_command=f"cd {DBT_DIR} && dbt run --select +tag:streaming",
    )

    dbt_test_streaming = BashOperator(
        task_id="dbt_test_streaming",
        bash_command=f"cd {DBT_DIR} && dbt test --select tag:streaming",
    )

    [
        telemetry_to_minio,
        realtime_kpis_to_minio,
        alerts_to_minio_and_kafka,
    ] >> minio_to_snowflake_raw >> dbt_run_streaming >> dbt_test_streaming
