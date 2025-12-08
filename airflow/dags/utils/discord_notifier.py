import re
import logging
import time
import json
import datetime

from airflow.models import Variable
from discord_webhook import DiscordWebhook, DiscordEmbed


def send_with_retry(webhook, max_retries=2):
    """
        Handle rate limit error from discord
    """
    for attempt in range(max_retries):
        response = webhook.execute()

        # If response is a dict with rate limit info
        try:
            parsed = json.loads(response)
        except Exception:
            parsed = None

        # Handle rate limit
        if parsed and "retry_after" in parsed:
            retry_after = float(parsed["retry_after"])
            logging.warning(f"Rate limited. Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
        
        # Success (no rate limit error)
        else:
            return response

        # exponential backoff safety
        time.sleep(1 * (attempt + 1))

    raise Exception("Discord webhook failed after retries")


def send_error_alert_discord(context):
    last_task = context.get('task_instance')
    task_name = last_task.task_id
    dag_name = last_task.dag_id
    log_link = last_task.log_url
    execution_date = str(context.get('logical_date'))

    try:
        error_message = str(context["exception"])
        error_message = error_message[:1000] + (error_message[1000:] and '...')

        str_start = re.escape("{'reason': ")
        str_end = re.escape('"}.')
        error_message = re.search('%s(.*)%s' % (str_start, str_end), error_message).group(1)
        error_message = "{'reason': " + error_message + '}'
    except:
        error_message = "Some error that cannot be extracted has occurred. Visit the logs!"

    webhook = DiscordWebhook(url=Variable.get("discord_webhook"))
    embed = DiscordEmbed(
        title="Airflow Alert - Task has failed!",
        color=0xCC0000,
        url=log_link,
        timestamp=execution_date
    )

    embed.add_embed_field(name="DAG", value=dag_name, inline=True)
    embed.add_embed_field(name="PRIORITY", value="HIGH", inline=True)
    embed.add_embed_field(name="TASK", value=task_name, inline=False)
    safe_error = (error_message or "No error message").strip()

    # limit the error message length
    if len(safe_error) > 1020:
        safe_error = safe_error[:1020] + "..."

    embed.add_embed_field(
        name="ERROR",
        value=safe_error,
        inline=False
    )

    webhook.add_embed(embed)

    response = send_with_retry(webhook)

    return response


def send_success_alert_discord(context):
    last_task = context.get('task_instance')
    task_name = last_task.task_id
    dag_name = last_task.dag_id
    execution_date = str(context.get('logical_date'))

    webhook = DiscordWebhook(url=Variable.get("discord_webhook"))
    embed = DiscordEmbed(
        title="DAG Run Succeeded!",
        description=f"**{dag_name}** completed successfully",
        color=0x00ff00,
        timestamp=execution_date
    )

    embed.set_author(
        name="Apache Airflow",
        url="https://airflow.apache.org",
        icon_url="https://airflow.apache.org/docs/apache-airflow/stable/favicon.png"
    )
    embed.add_embed_field(name="DAG", value=dag_name, inline=True)
    embed.add_embed_field(name="TASK", value=task_name, inline=False)
    
    airflow_webserver = Variable.get("airflow_webserver_url", "http://localhost:8080")
    dag_run_url = f"{airflow_webserver}/dags/{dag_name}/grid?dag_run_id={context['dag_run'].run_id}"
    embed.add_embed_field(name="View in Airflow", value=f"[Open DAG Run]({dag_run_url})", inline=False)

    webhook.add_embed(embed)

    # Small delay to avoid rate limits
    time.sleep(1)
    response = webhook.execute()

    if response.status_code != 204:
        raise ValueError(f"Discord webhook failed: {response.status_code} - {response.text}")

    return response
