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
COINS_MARKETS_ENDPOINT = "/coins/markets"
VS_CURRENCY = "eur"
PER_PAGE = 250  # Maximum allowed per_page by CoinGecko
RATE_LIMIT_DELAY = 1.2  # seconds between requests to respect rate limits

# Google Cloud Storage Configuration
GCS_BUCKET_NAME = "de-fi"
GCS_PREFIX = "markets_hourly"
DATA_PARTITION_DATE_FORMAT = "%Y-%m-%d"
DATA_PARTITION_HOUR_FORMAT = "%H"  # 24-hour format (00 to 23)

# Path to the CSV file containing the list of coins
COINS_CSV_PATH = os.path.join(os.path.dirname(__file__), 'id.csv')

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)

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
def fetch_market_data(vs_currency: str, page: int, per_page: int = 250) -> List[Dict]:
    """
    Fetch current market data for multiple coins using the /coins/markets endpoint with retry logic.
    :param vs_currency: The target currency of market data (e.g., 'usd', 'eur').
    :param page: Page number for paginated results.
    :param per_page: Number of results per page (max 250).
    :return: List of market data dictionaries.
    """
    url = f"{COINGECKO_API_BASE}{COINS_MARKETS_ENDPOINT}"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "sparkline": "false",
        "price_change_percentage": "24h,7d,14d,30d,1y"
    }
    response = requests.get(url, params=params)

    if response.status_code == 429:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            wait_time = int(retry_after)
            logger.warning(f"Received 429. Retry after {wait_time} seconds.")
            time.sleep(wait_time)
        else:
            logger.warning("Received 429. Retrying with exponential backoff.")
            # If no Retry-After header, tenacity's wait_exponential will handle
        response.raise_for_status()
    elif response.status_code >= 400:
        logger.error(f"Error {response.status_code}: {response.text}")
        response.raise_for_status()

    logger.info(f"Fetched market data for page {page}.")
    return response.json()

def write_parquet_to_gcs(df: pd.DataFrame, partition_date: str, partition_hour: str):
    """
    Write the DataFrame to GCS as a Parquet file partitioned by date and hour.
    Overwrites the partition if it already exists.
    :param df: The pandas DataFrame to write.
    :param partition_date: The date partition string in YYYY-MM-DD format.
    :param partition_hour: The hour partition string in HH format.
    """
    try:
        # Define the GCS path with date and hour partitions
        gcs_path = f"{GCS_PREFIX}/date={partition_date}/hour={partition_hour}/data.parquet"

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
        logger.error(f"Error writing Parquet to GCS for date {partition_date} and hour {partition_hour}: {e}")
        raise

def main() -> None:
    """
    Main function to orchestrate the ingestion process.
    """
    logger.info("Starting data ingestion for current market data.")

    # Determine the date and hour partitions (current UTC time)
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    partition_date = now_utc.strftime(DATA_PARTITION_DATE_FORMAT)
    partition_hour = now_utc.strftime(DATA_PARTITION_HOUR_FORMAT)

    logger.info(f"Data will be partitioned under date: {partition_date}, hour: {partition_hour}")

    # Read the list of coins from CSV
    try:
        coins = get_coins_from_csv(COINS_CSV_PATH)
    except Exception as e:
        logger.error(f"Failed to load coins from CSV: {e}")
        return

    all_market_data = []

    # Calculate total pages based on the number of coins and per_page
    total_coins = len(coins)
    total_pages = (total_coins // PER_PAGE) + (1 if total_coins % PER_PAGE != 0 else 0)
    logger.info(f"Total coins: {total_coins}, Total pages: {total_pages}")

    for page in range(1, total_pages + 1):
        logger.info(f"Fetching data for page {page}/{total_pages}")
        try:
            market_data = fetch_market_data(VS_CURRENCY, page, PER_PAGE)
            all_market_data.extend(market_data)
            logger.info(f"Successfully fetched data for page {page}.")
        except requests.exceptions.HTTPError as http_err:
            if http_err.response.status_code == 429:
                logger.error(f"Rate limit exceeded on page {page}. Will retry as per retry strategy.")
            else:
                logger.error(f"HTTP error on page {page}: {http_err}")
        except Exception as e:
            logger.error(f"Unexpected error on page {page}: {e}")

        # Throttle requests to respect rate limits
        logger.debug(f"Sleeping for {RATE_LIMIT_DELAY} seconds to respect rate limits.")
        time.sleep(RATE_LIMIT_DELAY)

    if not all_market_data:
        logger.warning("No market data fetched. Exiting.")
        return

    # Convert all_market_data to DataFrame
    logger.info("Converting fetched market data to DataFrame...")
    try:
        df = pd.DataFrame(all_market_data)
        # Add fetch_date and fetch_hour for partitioning
        df['fetch_date'] = partition_date
        df['fetch_hour'] = partition_hour
        logger.info(f"DataFrame created with {len(df)} records.")
    except Exception as e:
        logger.error(f"Error converting market data to DataFrame: {e}")
        return

    # Write DataFrame to GCS as Parquet
    logger.info("Writing Parquet file to GCS...")
    try:
        write_parquet_to_gcs(df, partition_date, partition_hour)
        logger.info("Data ingestion completed successfully.")
    except Exception as e:
        logger.error(f"Error writing Parquet to GCS: {e}")

if __name__ == "__main__":
    main()
