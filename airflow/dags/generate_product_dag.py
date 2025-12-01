import sys
import os

# Add external script directory to sys.path
external_script_path = '/opt/airflow/lib'
sys.path.append(external_script_path)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import duration
from lib.batch_pipeline.product_generator import generate_product
from utils.discord_notifier import send_alert_discord

with DAG(
    dag_id='product_generator',
    start_date = datetime(2025,11,24),
    schedule= '@hourly',
    catchup=True,
    description='product data generator and load to postgres db',
    tags=['data-loader','hourly'],
    default_args={"retries":1},
    dagrun_timeout=duration(minutes=10),
    on_failure_callback=send_alert_discord
)as dag:
    generate_product_task = PythonOperator(
        task_id='load_product',
        python_callable=generate_product,
        op_kwargs={'ts': '{{ ts }}'},
        on_failure_callback=send_alert_discord
    )