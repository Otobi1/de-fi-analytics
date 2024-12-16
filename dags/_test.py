
from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
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
PROJECT_ID = Variable.get("gcp_project_id")

TABLE_SCHEMA_OPERATOR = [
    {"name": "id", "type": "STRING", "mode": "NULLABLE"},
    {"name": "symbol", "type": "STRING", "mode": "NULLABLE"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "image", "type": "STRING", "mode": "NULLABLE"},
    {"name": "current_price", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "market_cap", "type": "INT64", "mode": "NULLABLE"},
    {"name": "market_cap_rank", "type": "INT64", "mode": "NULLABLE"},
    {"name": "market_cap_change_24h", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_volume", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fully_diluted_valuation", "type": "INT64", "mode": "NULLABLE"},
    {"name": "high_24h", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "low_24h", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "price_change_24h", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "price_change_percentage_24h", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "circulating_supply", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "total_supply", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "max_supply", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "market_cap_change_percentage_24h", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ath", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ath_change_percentage", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "ath_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "atl", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "atl_change_percentage", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "atl_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "roi", "type": "RECORD", "mode": "NULLABLE", "fields": [
        {"name": "times", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "currency", "type": "STRING", "mode": "NULLABLE"},
        {"name": "percentage", "type": "FLOAT", "mode": "NULLABLE"},
    ]},
    {"name": "last_updated", "type": "STRING", "mode": "NULLABLE"},
    {"name": "price_change_percentage_14d_in_currency", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "price_change_percentage_1y_in_currency", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "price_change_percentage_24h_in_currency", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "price_change_percentage_30d_in_currency", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "price_change_percentage_7d_in_currency", "type": "FLOAT", "mode": "NULLABLE"},
    {"name": "fetch_date", "type": "STRING", "mode": "NULLABLE"},
    {"name": "fetch_hour", "type": "STRING", "mode": "NULLABLE"},
]

TABLE_SCHEMA_CREATION = [
    bigquery.SchemaField(
        name=field["name"],
        field_type=field["type"],
        mode=field["mode"],
        fields=[
            bigquery.SchemaField(
                name=sub_field["name"],
                field_type=sub_field["type"],
                mode=sub_field["mode"],
            )
            for sub_field in field.get("fields", [])
        ] if field["type"] == "RECORD" else None,
    )
    for field in TABLE_SCHEMA_OPERATOR
]

CLUSTERING_FIELD = ["id"]


def create_default_args():
    return {
        'owner': 'tobi.olutunmbi',
        # 'depends_on_past': False,
        'start_date': datetime(2024, 12, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'email': ['tobiolutunmbi@gmail.com'],
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
    Checks if the BigQuery table exists. If not, creates it with clustering.
    """
    client = bigquery.Client(project=PROJECT_ID)
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
        table = bigquery.Table(table_full_id, schema=TABLE_SCHEMA_CREATION)
        table.clustering_fields = CLUSTERING_FIELD
        client.create_table(table)
        logger.info(f"Table `{table_full_id}` created with clustering on {CLUSTERING_FIELD}.")
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
        schema_fields=TABLE_SCHEMA_OPERATOR,
    )

    # Define Task Dependencies
    run_ingest >> check_and_create_table_task >> load_raw_data
