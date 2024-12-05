import os
import requests
import pandas as pd
from datetime import datetime, timezone
import logging
from google.cloud import storage

# ================== Configuration ==================

# API Configuration
API_URL = 'https://randomuser.me/api/'  # Default API URL

# GCS Configuration
GCS_BUCKET_NAME = 'de-fi'       # Name of your GCS bucket
GCS_BUCKET_FOLDER = 'test'       # Folder within the bucket

# Output Configuration
# Removed OUTPUT_DIR since we're uploading to GCS

# ===================================================

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')


def fetch_data(api_url):
    """
    Fetches data from the specified API URL.

    Args:
        api_url (str): The URL of the API to fetch data from.

    Returns:
        dict or list: The JSON response from the API.

    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
    """
    try:
        logging.info(f"Fetching data from API: {api_url}")
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        logging.info("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        raise


def save_data_to_gcs(data, bucket_name, folder, filename):
    """
    Saves the fetched data to a CSV file and uploads it to Google Cloud Storage.

    Args:
        data (dict or list): The data to be saved.
        bucket_name (str): Name of the GCS bucket.
        folder (str): Folder within the GCS bucket.
        filename (str): The name of the file to be created/uploaded.

    Raises:
        Exception: If there is an error during the saving or uploading process.
    """
    try:
        logging.info("Processing data for CSV conversion.")
        # Inspect the data structure before creating DataFrame
        logging.debug(f"Data type: {type(data)}")
        logging.debug(f"Data content: {data}")

        # Handle different data structures appropriately
        if isinstance(data, dict) and 'results' in data:
            results = data['results']
            df = pd.json_normalize(results)
        elif isinstance(data, list):
            # If it's a list of dictionaries
            if all(isinstance(item, dict) for item in data):
                df = pd.json_normalize(data)
            else:
                # If list contains non-dict items, wrap them in dicts
                df = pd.DataFrame({'data': data})
        elif isinstance(data, dict):
            # If it's a single dictionary, convert to DataFrame with one row
            df = pd.json_normalize([data])
        else:
            # For other data types, convert to DataFrame with a single column
            df = pd.DataFrame({'data': [data]})

        # Convert DataFrame to CSV string
        csv_data = df.to_csv(index=False)

        # Initialize GCS client using Application Default Credentials
        logging.info("Initializing Google Cloud Storage client with default credentials.")
        client = storage.Client()

        # Reference the bucket
        bucket = client.bucket(bucket_name)
        if not bucket.exists():
            logging.error(f"GCS bucket '{bucket_name}' does not exist.")
            raise ValueError(f"GCS bucket '{bucket_name}' does not exist.")

        # Define the blob path
        blob_path = f"{folder}/{filename}"
        blob = bucket.blob(blob_path)

        # Upload the CSV data
        logging.info(f"Uploading data to GCS bucket '{bucket_name}' at '{blob_path}'.")
        blob.upload_from_string(csv_data, content_type='text/csv')
        logging.info(f"Data successfully uploaded to gs://{bucket_name}/{blob_path}")
    except Exception as e:
        logging.error(f"Error saving data to GCS: {e}")
        raise


def main():
    """
    Main function to orchestrate data fetching and saving to GCS.
    """
    try:
        # Data ingestion logic
        api_url = API_URL  # Using predefined API URL
        if not api_url:
            logging.error("API_URL is not set.")
            raise ValueError("API_URL is not set.")

        data = fetch_data(api_url)

        # Replace deprecated utcnow() with timezone-aware datetime
        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        filename = f'data_ingested_{timestamp}.csv'  # Changed to .csv for consistency

        # Save data to GCS
        save_data_to_gcs(
            data=data,
            bucket_name=GCS_BUCKET_NAME,
            folder=GCS_BUCKET_FOLDER,
            filename=filename
        )

        print(f'Data successfully uploaded to gs://{GCS_BUCKET_NAME}/{GCS_BUCKET_FOLDER}/{filename}')
    except Exception as e:
        logging.error(f"An error occurred in the ingestion process: {e}")
        exit(1)


if __name__ == '__main__':
    main()
