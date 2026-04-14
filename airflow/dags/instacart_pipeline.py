from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

PROJECT_DIR = '/Users/nithinpradeep/Documents/product-analytics-platform'
DBT_DIR = f'{PROJECT_DIR}/dbt/instacart_analytics'

with DAG(
    dag_id='instacart_pipeline',
    description='Instacart end-to-end pipeline: ingest → dbt run → dbt test',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=['instacart', 'product-analytics']
) as dag:

    ingest = BashOperator(
        task_id='ingest_raw_data',
        bash_command=f'cd {PROJECT_DIR} && python ingestion/load_instacart.py',
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=f'cd {DBT_DIR} && DBT_PROFILES_DIR=~/.dbt dbt run',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=f'cd {DBT_DIR} && DBT_PROFILES_DIR=~/.dbt dbt test',
    )

    # Define dependencies — ingest first, then run, then test
    ingest >> dbt_run >> dbt_test