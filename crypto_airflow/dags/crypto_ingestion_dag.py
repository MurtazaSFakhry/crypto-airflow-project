from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.append("/opt/airflow/scripts")
from ingest_hourly_bars import ingest_hourly_bars

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="crypto_hourly_ingestion",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="0 * * * *",  # every hour
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_crypto_hourly",
        python_callable=ingest_hourly_bars,
    )
