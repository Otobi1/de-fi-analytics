import requests
import pandas as pd
import datetime
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from typing import List, Dict
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# CoinGecko API Configuration
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
MARKET_CHART_ENDPOINT_TEMPLATE = "/coins/{id}/market_chart"
VS_CURRENCY = "eur"
DAYS = 10  # Number of days to fetch data for

# Google Cloud Storage Configuration
GCS_BUCKET_NAME = "de-fi"
GCS_PREFIX = "market_data"
DATA_PARTITION_FORMAT = "%Y-%m-%d"

# Path to the CSV file containing the list of coins
COINS_CSV_PATH = os.path.join(os.path.dirname(__file__), 'id.csv')

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

# Define rate limiter: Adjust based on APIs rate limits
RATE_LIMIT_DELAY = 1.2  # seconds


def get_coins_from_csv(csv_path: str) -> List[Dict]:
    """
    Read the list of coins from a CSV file.
    :param csv_path: Path to the CSV file.
    :return: List of coin dictionaries with 'coin_id' and 'coin_name'.
    """
    try:
        df = pd.read_csv(csv_path)
        # Standardize column names to lowercase to avoid case sensitivity issues
        df.columns = [col.strip().lower() for col in df.columns]

        # Rename columns if necessary
        if 'id' in df.columns and 'coin_id' not in df.columns:
            df.rename(columns={'id': 'coin_id'}, inplace=True)
        elif 'coinid' in df.columns and 'coin_id' not in df.columns:
            df.rename(columns={'coinid': 'coin_id'}, inplace=True)

        coins = df.to_dict(orient='records')
        # Verify that each coin has a 'coin_id'
        valid_coins = [coin for coin in coins if 'coin_id' in coin and pd.notna(coin['coin_id'])]
        invalid_coins = [coin for coin in coins if 'coin_id' not in coin or pd.isna(coin['coin_id'])]

        if invalid_coins:
            logger.warning(f"{len(invalid_coins)} coins are missing 'coin_id' and will be skipped.")
            for coin in invalid_coins:
                logger.warning(f"Invalid coin entry: {coin}")

        logger.info(f"Loaded {len(valid_coins)} valid coins from CSV.")
        return valid_coins
    except Exception as e:
        logger.error(f"Error reading CSV file at {csv_path}: {e}")
        raise


@retry(
    retry=retry_if_exception_type(requests.exceptions.HTTPError),
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60)
)
def get_market_chart(coin_id: str, days: int = 1) -> Dict:
    """
    Fetch market chart data for a given coin ID with retry logic and Retry-After handling.
    :param coin_id: The CoinGecko coin ID.
    :param days: Number of days to fetch data for.
    :return: Market chart data as a dictionary.
    """
    endpoint = MARKET_CHART_ENDPOINT_TEMPLATE.format(id=coin_id)
    url = f"{COINGECKO_API_BASE}{endpoint}"
    params = {
        "vs_currency": VS_CURRENCY,
        "days": days,
        "interval": "daily"
    }
    response = requests.get(url, params=params)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            wait_time = int(retry_after)
            logger.warning(f"Received 429 for {coin_id}. Retry after {wait_time} seconds.")
            time.sleep(wait_time)
        else:
            logger.warning(f"Received 429 for {coin_id}. Retrying with exponential backoff.")
            # If no Retry-After header, tenacity's wait_exponential will handle
        response.raise_for_status()
    elif response.status_code >= 400:
        logger.error(f"Error {response.status_code} for {coin_id}: {response.text}")
        response.raise_for_status()

    logger.info(f"Fetched market chart for {coin_id}.")
    return response.json()


def write_parquet_to_gcs(df: pd.DataFrame, partition_date: str):
    """
    Write the DataFrame to GCS as a Parquet file partitioned by date.
    Overwrites the partition if it already exists.
    :param df: The pandas DataFrame to write.
    :param partition_date: The date partition string in YYYY-MM-DD format.
    """
    # Define the GCS path
    gcs_path = f"{GCS_PREFIX}/date={partition_date}/data.parquet"

    # Convert DataFrame to Parquet in memory
    table = pa.Table.from_pandas(df)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    buffer.seek(0)

    # Upload to GCS, overwriting if exists
    blob = bucket.blob(gcs_path)
    blob.upload_from_file(buffer, content_type='application/octet-stream')
    logger.info(f"Written data to gs://{GCS_BUCKET_NAME}/{gcs_path}")


def main() -> None:
    """
    Main function to orchestrate the ingestion process.
    """
    # Determine the date partition (yesterday)
    today = datetime.datetime.now(datetime.timezone.utc).date()
    yesterday = today - datetime.timedelta(days=1)
    partition_date = yesterday.strftime(DATA_PARTITION_FORMAT)

    logger.info(f"Starting data ingestion for date: {partition_date}")

    # Read the list of coins from CSV
    try:
        coins = get_coins_from_csv(COINS_CSV_PATH)
    except Exception as e:
        logger.error(f"Failed to load coins from CSV: {e}")
        return

    all_data = []

    # Sequentially fetch data for each coin
    for idx, coin in enumerate(coins, start=1):
        coin_id = coin['coin_id']
        logger.info(f"Processing coin {idx}/{len(coins)}: {coin_id}")
        try:
            market_chart = get_market_chart(coin_id, DAYS)
            market_chart['coin_id'] = coin_id
            all_data.append(market_chart)
            logger.info(f"Successfully fetched data for {coin_id}.")
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 429:
                logger.error(f"Rate limit exceeded for {coin_id}. Will retry as per retry strategy.")
            else:
                logger.error(f"HTTP error for {coin_id}: {http_err}")
        except Exception as e:
            logger.error(f"Unexpected error for {coin_id}: {e}")

        # Throttle requests to respect rate limits
        logger.debug(f"Sleeping for {RATE_LIMIT_DELAY} seconds to respect rate limits.")
        time.sleep(RATE_LIMIT_DELAY)

    if not all_data:
        logger.warning("No data fetched. Exiting.")
        return

    # Convert all_data to DataFrame
    logger.info("Converting data to DataFrame...")
    try:
        df = pd.json_normalize(all_data, sep='_')
        df['date'] = partition_date
        logger.info(f"DataFrame created with {len(df)} records.")
    except Exception as e:
        logger.error(f"Error converting data to DataFrame: {e}")
        return

    # Write DataFrame to GCS as Parquet
    logger.info("Writing Parquet file to GCS...")
    try:
        write_parquet_to_gcs(df, partition_date)
        logger.info("Data ingestion completed successfully.")
    except Exception as e:
        logger.error(f"Error writing Parquet to GCS: {e}")
