from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
import pandas as pd

def get_field_type(dtype):
    match dtype:
        case 'object':
            return 'STRING'
        case 'int64':
            return 'INTEGER'
        case 'float64':
            return 'FLOAT'
        case 'datetime64[ns]':
            return 'TIMESTAMP'
        case 'bool':
            return 'BOOLEAN'
        case _:
            return 'STRING'

def extract_all_tables(**kwargs):
    
    pg_hook = PostgresHook(postgres_conn_id='postgres_hook')
    tables = ['users', 'products','orders']  
    data = {}
    
    for table in tables:
        query = f"""
            SELECT * FROM {table}
            WHERE DATE(created_at) = '2025-10-10'
        """
        df = pg_hook.get_pandas_df(query)
        data[table] = df
        
        
    
    for table,df in data.items():
        print(f'table : {table}')
        for column_name, dtype in df.dtypes.items():
            print(f'data type before= {column_name} : {dtype}')
            field_type = get_field_type(dtype)
            print(f'data type after= {column_name} : {field_type}')
        print()
        print("-------------------------------------------")