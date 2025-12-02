from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
from datetime import datetime, timedelta
from lib.batch_pipeline.utils.bq_utils import get_field_type

def extract_source_data(filter_date):
    """
        Extract raw data from source PostgreSQL tables for a specific date.

        This function is used in ETL/data warehousing pipelines to pull daily incremental 
        data from source databases.
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_hook')
    tables = ['users', 'products','orders']  
    data = {}
    
    for table in tables:
        if table != "orders":
            query = f"""
                SELECT * FROM {table}
                WHERE DATE(created_at) = '{filter_date}'
            """
        else:
            query = f"""
                SELECT * FROM {table}
            """
        df = pg_hook.get_pandas_df(query)
        data[table] = df
    
    return data

def load_to_bigquery(**kwargs):
    """
        Function to extract daily incremental data from PostgreSQL 
        and load it into Google BigQuery (Bronze layer) with deduplication.

        This function is designed to run daily in an Airflow DAG. It:
        1. Pulls data created on the previous day (execution_date - 1)
        2. Loads into partitioned BigQuery tables (by `created_at` DATE)
        3. Performs upsert (MERGE) logic to handle duplicates and late-arriving data
    """
    execution_date = kwargs['logical_date']
    filter_date = (execution_date - timedelta(days=1)).date()
    data = extract_source_data(filter_date)
    
    bq_hook = BigQueryHook(gcp_conn_id='gcp_bigquery_hook', use_legacy_sql=False)
    client = bq_hook.get_client()
    
    project_id = 'jcdeah-006'
    dataset_id = 'akmal_ecommerce_bronze_finpro'
    
    for table, df in data.items():
        table_id = f'{project_id}.{dataset_id}.{table}'
        table_ref = bigquery.TableReference.from_string(table_id)
        
        # Schema partitioning: Partition by created_at
        schema = []
        table_columns = df.columns
        primary_keys = table_columns[0]

        for column_name, dtype in df.dtypes.items():
            # Map Pandas dtype to BigQuery field type 
            field_type = get_field_type(dtype)
            schema.append(bigquery.SchemaField(column_name, field_type))
        
        partition_field = 'created_at'
        table = bigquery.Table(table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        
        # Create table if not exist
        try:
            client.get_table(table_ref)
        except:
            client.create_table(table)
        
        # Incremental load: Using job for upsert (merge)
        # Append first, then merge
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  
            schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        )
        
        # Load DF to BigQuery
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()  # Wait till finish
        
        # Query for inserting values
        columns = ', '.join(df.columns)
        update_columns = ', '.join([f'T.{col} = S.{col}' for col in df.columns if col != primary_keys])
        insert_clause = f"INSERT ({columns}) VALUES ({', '.join([f'S.{col}' for col in df.columns])})"

        merge_query = f"""
            MERGE `{table_id}` T
            USING (
                SELECT * FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {primary_keys} ORDER BY created_at DESC) rn
                    FROM `{table_id}`
                    WHERE DATE(created_at) = '{filter_date}'
                )
                WHERE rn = 1
            ) S
            ON T.{primary_keys} = S.{primary_keys}
            WHEN MATCHED THEN
                UPDATE SET {update_columns}
            WHEN NOT MATCHED THEN
                {insert_clause}
        """
        client.query(merge_query).result()  
       