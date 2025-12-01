from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
hook = BigQueryHook(gcp_conn_id='gcp_bigquery_hook')
client = hook.get_client()
print(client.project)