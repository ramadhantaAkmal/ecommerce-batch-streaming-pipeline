from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import duration
from utils.discord_notifier import send_error_alert_discord,send_success_alert_discord

DBT_DIR = "/opt/airflow/lib/batch_pipeline/dwh_transform/ecommerce_batch_stream_pipeline"

with DAG(
    dag_id="dbt_dwh_daily",
    start_date=datetime(2025, 11, 25),
    schedule='@daily',
    catchup=False,
    description="Create silver layer and gold layer",
    tags=["daily", "bash_op"],
    default_args={"retries":1},
    dagrun_timeout=duration(minutes=5),
    on_failure_callback=send_error_alert_discord,
    on_success_callback=send_success_alert_discord
):
    silver_layer = BashOperator(
        task_id="silver_layer",
        bash_command=f"cd {DBT_DIR} && dbt run -s akmal_ecommerce_silver_finpro --target silver"
    )
    
    gold_layer = BashOperator(
        task_id="gold_layer",
        bash_command=f"cd {DBT_DIR} && dbt run -s akmal_ecommerce_gold_finpro --target gold"
    )
    
    silver_layer >> gold_layer