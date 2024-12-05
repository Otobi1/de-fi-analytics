
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import ingest

# Default arguments for the DAG
default_args = {
    'owner': 't.o',
    'depends_on_past': False,
    'email': ['tobi.olutunmbi@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': datetime.timedelta(minutes=5),
}

def run_ingest():
    ingest.main()


dag = DAG(
    'daily_coingecko_processing',
    default_args=default_args,
    description='Ingest CoinGecko data into Google Cloud Storage as Parquet files',
    schedule_interval='@daily',
    start_date=days_ago(10),
    catchup=True,
)

ingest_task = PythonOperator(
    task_id='ingest_coingecko_to_gcs',
    python_callable=run_ingest,
    provide_context=True,
    dag=dag,
)
