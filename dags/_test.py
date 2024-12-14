from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from google.cloud import bigquery
from airflow.models import Variable
from datetime import datetime, timedelta
import ingest_hourly


# Constants
DATASET_ID = "de_fi_analytics"
RAW_TABLE_ID = "de_fi_hourly"
GCS_BUCKET = "de-fi"
GCS_PATH = "markets_hourly/*.parquet"
PARTITION_FIELD = "fetch_date"


def create_default_args():
    return {
        'owner': 'tobi.olutunmbi',
        'depends_on_past': False,
        'start_date': datetime(2024, 12, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }


def execute_ingest():
    """
    Executes the ingestion operation.
    """
    ingest_hourly.main()


def check_and_create_table(**kwargs):
    """
    Checks if the BigQuery table exists. If not, creates it with the specified partitioning.
    """
    client = bigquery.Client()
    table_id = f"{DATASET_ID}.{RAW_TABLE_ID}"

    try:
        client.get_table(table_id)
        print(f"Table `{table_id}` already exists.")
    except NotFound:
        table = bigquery.Table(table_id)
        table.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=PARTITION_FIELD,
        )
        client.create_table(table)
        print(f"Table `{table_id}` created with partitioning on `{PARTITION_FIELD}`.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        raise

# def create_agg_table_if_not_exists(**kwargs):
#     client = bigquery.Client()
#     table_id = f"{DATASET_ID}.aggregated_table"
#
#     try:
#         client.get_table(table_id)
#         print(f"Aggregation Table {table_id} already exists.")
#     except Exception:
#         # Define schema for aggregation table
#         schema = [
#             bigquery.SchemaField("category", "STRING"),
#             bigquery.SchemaField("total_count", "INTEGER"),
#             bigquery.SchemaField("total_amount", "FLOAT"),
#             # Add other aggregation fields as needed
#         ]
#         table = bigquery.Table(table_id, schema=schema)
#         client.create_table(table)
#         print(f"Aggregation Table {table_id} created with defined schema.")


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
        autodetect=True,
        time_partitioning={
            "type": "DAY",
            "field": PARTITION_FIELD,
        },
    )


    # # Check Aggregation Table Exists
    # check_agg_table = BigQueryCheckOperator(
    #     task_id='check_aggregation_table_exists',
    #     sql=f"""
    #             SELECT COUNT(*)
    #             FROM `{DATASET_ID}.INFORMATION_SCHEMA.TABLES`
    #             WHERE table_name = '{AGG_TABLE_ID}'
    #         """,
    #     use_legacy_sql=False,
    # )


    # # Create Aggregation Table if Not Exists
    # create_agg_table = PythonOperator(
    #     task_id='create_agg_table_if_not_exists',
    #     python_callable=create_agg_table_if_not_exists,
    #     provide_context=True,
    # )

    # # Perform Aggregations and Append to Aggregation Table
    # aggregate_data = BigQueryInsertJobOperator(
    #     task_id='aggregate_data',
    #     configuration={
    #         "query": {
    #             "query": f"""
    #                     INSERT INTO `{DATASET_ID}.{AGG_TABLE_ID}` (category, total_count, total_amount)
    #                     SELECT
    #                         category,
    #                         COUNT(*) AS total_count,
    #                         SUM(amount) AS total_amount
    #                     FROM
    #                         `{DATASET_ID}.{RAW_TABLE_ID}`
    #                     WHERE
    #                        {PARTITION_FIELD} >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
    #                     GROUP BY
    #                         category
    #                 """,
    #             "useLegacySql": False,
    #         }
    #     },
    # )


    # Define Task Dependencies
    run_ingest >> check_raw_table >> create_raw_table >> load_raw_data
    # check_agg_table >> create_agg_table_if_not_exists >> aggregate_data
    # load_raw_data >> aggregate_data
