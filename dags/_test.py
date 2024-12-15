from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.api_core.exceptions import NotFound  # Added import
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import ingest_hourly


logger = logging.getLogger(__name__)

# Constants
DATASET_ID = "de_fi_analytics"
RAW_TABLE_ID = "de_fi_hourly"
GCS_BUCKET = "de-fi"
GCS_PATH = "markets_hourly/*.parquet"
PARTITION_FIELD = "fetch_date"

TABLE_SCHEMA = [
    bigquery.SchemaField("id", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("symbol", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("name", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("image", "STRING", mode="NULLABLE"),
    bigquery.SchemaField("current_price", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("market_cap", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("market_cap_change_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total_volume", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("fully_diluted_valuation", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("high_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("low_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_change_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_change_percentage_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("circulating_supply", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("total_supply", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("max_supply", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("market_cap_change_percentage_24h", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ath", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ath_change_percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("ath_date", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("atl", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("atl_change_percentage", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("atl_date", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("roi", "FLOAT", mode="NULLABLE"),
    # bigquery.SchemaField("roi.percentage", "FLOAT", mode="NULLABLE"),
    # bigquery.SchemaField("roi.times", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("last_updated", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_change_percentage_14d_in_currency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_change_percentage_1y_in_currency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_change_percentage_24h_in_currency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_change_percentage_30d_in_currency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("price_change_percentage_7d_in_currency", "FLOAT", mode="NULLABLE"),
    bigquery.SchemaField("fetch_date", "DATE", mode="NULLABLE"),
    bigquery.SchemaField("fetch_hour", "FLOAT", mode="NULLABLE"),
]


def create_default_args():
    return {
        'owner': 'tobi.olutunmbi',
        'depends_on_past': False,
        'start_date': datetime(2024, 12, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email': ['tobiolutunmbi@gmail.com'],
        'email_on_failure': True,
        'email_on_retry': False,
    }


def execute_ingest():
    """
    Executes the ingestion operation.
    """
    try:
        ingest_hourly.main()
        logger.info("Data ingestion completed successfully.")
    except Exception as e:
        logger.error(f"Data ingestion failed: {e}")
        raise


def check_and_create_table(**kwargs):
    """
    Checks if the BigQuery table exists. If not, creates it with the specified partitioning.
    """
    client = bigquery.Client(project='sapient-hub-442421-b5')
    project_id = client.project
    dataset_full_id = f"{project_id}.{DATASET_ID}"
    table_full_id = f"{dataset_full_id}.{RAW_TABLE_ID}"

    try:
        client.get_dataset(dataset_full_id)
        logger.info(f"Dataset `{dataset_full_id}` exists.")
    except NotFound:
        dataset = bigquery.Dataset(dataset_full_id)
        client.create_dataset(dataset)
        logger.info(f"Dataset `{dataset_full_id}` created.")

    try:
        client.get_table(table_full_id)
        logger.info(f"Table `{table_full_id}` already exists.")
    except NotFound:
        table = bigquery.Table(table_full_id, schema=TABLE_SCHEMA)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=PARTITION_FIELD,
        )
        client.create_table(table)
        logger.info(f"Table `{table_full_id}` created with partitioning on `{PARTITION_FIELD}`.")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        raise


with DAG(
        '_test',
        schedule_interval='@hourly',
        default_args=create_default_args(),
        catchup=False,
        description='A DAG to ingest CoinGecko data from CSV list into GCS',
) as dag:


    run_ingest = PythonOperator(
        task_id='run_ingest_data',
        python_callable=execute_ingest,
    )

    # Check if Raw Table Exists
    check_and_create_table_task = PythonOperator(
        task_id='check_and_create_raw_table',
        python_callable=check_and_create_table,
    )


    # Load Data into Raw Table
    load_raw_data = GCSToBigQueryOperator(
        task_id='load_raw_data',
        bucket=GCS_BUCKET,
        source_objects=[GCS_PATH],
        destination_project_dataset_table=f"{DATASET_ID}.{RAW_TABLE_ID}",
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        schema_fields=TABLE_SCHEMA,
        time_partitioning={
            "type": "DAY",
            "field": PARTITION_FIELD,
        },
    )

    # Define Task Dependencies
    run_ingest >> check_and_create_table_task >> load_raw_data
