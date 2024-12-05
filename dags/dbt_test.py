from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts import ingest_data

default_args = {
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def run_ingest_data():
    ingest_data.main()


with DAG(
        'dbt_python_dag',
        schedule_interval='@daily',
        default_args=default_args,
        catchup=False,
        description='A DAG to run ingest_data.py using PythonOperator',
) as dag:
    run_ingest = PythonOperator(
        task_id='run_ingest_data',
        python_callable=run_ingest_data,
    )
