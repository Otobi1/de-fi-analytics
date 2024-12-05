import requests
import pandas as pd
import datetime
from google.cloud import storage
import pyarrow as pa
import pyarrow.parquet as pq
import io
import os
from typing import List, Dict


# CoinGecko API Configuration
COINGECKO_API_BASE = "https://api.coingecko.com/api/v3"
TOP_COINS_ENDPOINT = "/coins/markets"
MARKET_CHART_ENDPOINT_TEMPLATE = "/coins/{id}/market_chart"
VS_CURRENCY = "eur"
ORDER = "market_cap_desc"
PER_PAGE = 100
PAGE = 1
DAYS = 10  # Number of days to fetch data for. 1 for previous day.

# Google Cloud Storage Configuration
GCS_BUCKET_NAME = "de-fi"
GCS_PREFIX = "market_data"
DATA_PARTITION_FORMAT = "%Y-%m-%d"

# Initialize GCS client
storage_client = storage.Client()
bucket = storage_client.bucket(GCS_BUCKET_NAME)



def get_top_coins() -> List[Dict]:
    """
    Get the top 100 coins by market cap from CoinGecko.
    """
    url = f"{COINGECKO_API_BASE}{TOP_COINS_ENDPOINT}"
    params = {
        "vs_currency": VS_CURRENCY,
        "order": ORDER,
        "per_page": PER_PAGE,
        "page": PAGE,
        "sparkline": False
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
    coins = response.json()
    return coins

def get_market_chart(coin_id: str, days: int = 1) -> Dict:
    """
    Fetch market chart data for a given coin ID.
    :param coin_id: The CoinGecko coin ID.
    :param days: Number of days to fetch data for.
    """
    endpoint = MARKET_CHART_ENDPOINT_TEMPLATE.format(id=coin_id)
    url = f"{COINGECKO_API_BASE}{endpoint}"
    params = {
        "vs_currency": VS_CURRENCY,
        "days": days,
        "interval": "daily"
    }
    response = requests.get(url, params=params)
    response.raise_for_status()
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
    print(f"Written data to gs://{GCS_BUCKET_NAME}/{gcs_path}")


def main() -> object:
    # Determine the date partition (yesterday)
    today = datetime.datetime.now(datetime.timezone.utc).date()
    yesterday = today - datetime.timedelta(days=1)
    partition_date = yesterday.strftime(DATA_PARTITION_FORMAT)

    print(f"Starting data ingestion for date: {partition_date}")

    # Fetch top 100 coins
    print("Fetching top 100 coins by market cap...")
    try:
        top_coins = get_top_coins()
        print(f"Retrieved {len(top_coins)} coins.")
    except Exception as e:
        print(f"Error fetching top coins: {e}")
        return

    all_data = []

    # Iterate through each coin and fetch market chart data
    for idx, coin in enumerate(top_coins, start=1):
        coin_id = coin['id']
        print(f"[{idx}/{len(top_coins)}] Fetching market chart for '{coin_id}'...")
        try:
            market_chart = get_market_chart(coin_id, days=DAYS)
            # Attach the coin_id to the market_chart data
            market_chart['coin_id'] = coin_id
            # Append to all_data
            all_data.append(market_chart)
        except Exception as e:
            print(f"Error fetching data for {coin_id}: {e}")
            continue

    if not all_data:
        print("No data fetched. Exiting.")
        return

    # Convert all_data to DataFrame
    print("Converting data to DataFrame...")
    try:
        df = pd.json_normalize(all_data, sep='_')
        df['date'] = partition_date
        print(f"DataFrame created with {len(df)} records.")
    except Exception as e:
        print(f"Error converting data to DataFrame: {e}")
        return

    # Write DataFrame to GCS as Parquet
    print("Writing Parquet file to GCS...")
    try:
        write_parquet_to_gcs(df, partition_date)
        print("Data ingestion completed successfully.")
    except Exception as e:
        print(f"Error writing Parquet to GCS: {e}")

if __name__ == "__main__":
    main()
