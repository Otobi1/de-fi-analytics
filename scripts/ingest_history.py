import requests
import pandas as pd
import datetime
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from typing import List, Dict, Hashable
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
DAYS = 365 # Fetches all available historical data

# Google Cloud Storage Configuration
GCS_BUCKET_NAME = "de-fi"
GCS_PREFIX = "market_data"
DATA_PARTITION_FORMAT = "%Y-%m-%d"

# Path to the CSV file containing the list of coins
COINS_CSV_PATH = os.path.join(os.path.dirname(__file__), 'id.csv')

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

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
    :param days: Number of days to fetch data for or 'max' for full history.
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

def process_market_chart(market_chart: Dict, coin_id: str) -> pd.DataFrame:
    """
    Process raw market chart data into a DataFrame with proper date partitioning.
    :param market_chart: Raw market chart data from CoinGecko.
    :param coin_id: The CoinGecko coin ID.
    :return: Processed DataFrame with 'date', 'coin_id', and relevant metrics.
    """
    try:
        prices = market_chart.get('prices', [])
        market_caps = market_chart.get('market_caps', [])
        total_volumes = market_chart.get('total_volumes', [])

        # Convert lists to DataFrames
        df_prices = pd.DataFrame(prices, columns=['timestamp', 'price'])
        df_market_caps = pd.DataFrame(market_caps, columns=['timestamp', 'market_cap'])
        df_total_volumes = pd.DataFrame(total_volumes, columns=['timestamp', 'total_volume'])

        # Merge DataFrames on timestamp
        df = df_prices.merge(df_market_caps, on='timestamp', how='left').merge(df_total_volumes, on='timestamp', how='left')

        # Convert timestamp to date
        df['date'] = pd.to_datetime(df['timestamp'], unit='ms').dt.strftime(DATA_PARTITION_FORMAT)

        # Add coin_id
        df['coin_id'] = coin_id

        # Drop the original timestamp
        df.drop(columns=['timestamp'], inplace=True)

        return df
    except Exception as e:
        logger.error(f"Error processing market chart data for {coin_id}: {e}")
        raise

def write_parquet_to_gcs(df: pd.DataFrame, partition_date: str):
    """
    Write the DataFrame to GCS as a Parquet file partitioned by date.
    Overwrites the partition if it already exists.
    :param df: The pandas DataFrame to write.
    :param partition_date: The date partition string in YYYY-MM-DD format.
    """
    try:
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
    except Exception as e:
        logger.error(f"Error writing Parquet to GCS for date {partition_date}: {e}")
        raise

def main() -> None:
    """
    Main function to orchestrate the ingestion process.
    """
    logger.info("Starting data ingestion for full history.")

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
            df = process_market_chart(market_chart, coin_id)
            all_data.append(df)
            logger.info(f"Successfully fetched and processed data for {coin_id}.")
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

    # Combine all DataFrames
    logger.info("Combining all fetched data into a single DataFrame...")
    try:
        combined_df = pd.concat(all_data, ignore_index=True)
        logger.info(f"Combined DataFrame created with {len(combined_df)} records.")
    except Exception as e:
        logger.error(f"Error combining DataFrames: {e}")
        return

    # Validate that 'date' column exists
    if 'date' not in combined_df.columns:
        logger.error("The combined DataFrame does not contain a 'date' column. Exiting.")
        return

    # Group data by date
    logger.info("Grouping data by date for partitioned writing...")
    grouped = combined_df.groupby('date')

    # Iterate over each group and write to GCS
    partition_date: str
    for partition_date, group_df in grouped:
        logger.info(f"Writing data for date: {partition_date} with {len(group_df)} records.")
        try:
            write_parquet_to_gcs(group_df, partition_date)
        except Exception as e:
            logger.error(f"Failed to write data for date {partition_date}: {e}")

    logger.info("Data ingestion completed successfully.")

if __name__ == "__main__":
    main()
