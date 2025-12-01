import re
import logging
import time
import json
import datetime

from airflow.models import TaskInstance, Variable
from discord_webhook import DiscordWebhook, DiscordEmbed


def send_with_retry(webhook, max_retries=2):
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


def send_alert_discord(context):
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
        color='CC0000',
        url=log_link,
        timestamp=execution_date
    )

    embed.add_embed_field(name="DAG", value=dag_name, inline=True)
    embed.add_embed_field(name="PRIORITY", value="HIGH", inline=True)
    embed.add_embed_field(name="TASK", value=task_name, inline=False)
    safe_error = (error_message or "No error message").strip()

    # Discord max embed field value = 1024 chars
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
