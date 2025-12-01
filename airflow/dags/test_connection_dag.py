import sys
import os

# Add external script directory to sys.path
external_script_path = '/opt/airflow/tests'
sys.path.append(external_script_path)

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
from extract_datasource_test import extract_all_tables
from utils.discord_notifier import send_alert_discord

with DAG(
    dag_id='test_conn',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['test'],
    on_failure_callback=send_alert_discord
) as dag:
    
    test_bq_conn = BigQueryGetDatasetOperator(
        task_id='test_bq_conn',
        dataset_id='akmal_ecommerce_bronze_finpro',
        gcp_conn_id='gcp_bigquery_hook',
        on_failure_callback=send_alert_discord
    )
    
    postgres_test_conn = PythonOperator(
        task_id='test_postgres_conn',
        python_callable=extract_all_tables,
        dag=dag,
        on_failure_callback=send_alert_discord
    )
    
    postgres_test_conn>> test_bq_conn
