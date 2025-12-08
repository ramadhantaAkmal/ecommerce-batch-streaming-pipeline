import sys
import os

# Add external script directory to sys.path
external_script_path = '/opt/airflow/lib'
sys.path.append(external_script_path)

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import bigquery
from pendulum import duration
from batch_pipeline.load_bronze_bq import load_to_bigquery
from utils.discord_notifier import send_error_alert_discord,send_success_alert_discord

with DAG(
    dag_id='bronze_loader',
    start_date = datetime(2025,11,24),
    schedule= '@daily',
    catchup=True,
    description='preparation/raw data load to bigquery',
    tags=['bq-loader','daily'],
    default_args={"retries":1},
    dagrun_timeout=duration(minutes=5),
    on_failure_callback=send_error_alert_discord,
    on_success_callback=send_success_alert_discord
)as dag:
    load_bronze_bq = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
    )