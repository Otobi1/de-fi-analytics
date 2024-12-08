
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import ingest

default_args = {
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def execute_ingest():
    ingest.main()

with DAG(
        'daily_coingecko_ingest',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False,
        description='A DAG to ingest CoinGecko data from CSV list into GCS',
) as dag:
    run_ingest = PythonOperator(
        task_id='run_ingest_data',
        python_callable=execute_ingest,
    )
