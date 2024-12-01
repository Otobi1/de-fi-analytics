import os
import requests
import pandas as pd
from datetime import datetime, timezone
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')

def fetch_data(api_url):
    try:
        logging.info(f"Fetching data from API: {api_url}")
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        logging.info("Data fetched successfully.")
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from API: {e}")
        raise

def save_data(data, filename):
    try:
        logging.info(f"Saving data to {filename}")
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

        df.to_csv(filename, index=False)
        logging.info(f"Data saved to {filename}")
    except Exception as e:
        logging.error(f"Error saving data to {filename}: {e}")
        raise

def main():
    try:
        # Your data ingestion logic
        api_url = 'https://randomuser.me/api/'
        data = fetch_data(api_url)

        timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
        filename = f'data_ingested_{timestamp}.txt'
        save_data(data, filename)
        print(f'Data saved to {filename}')
    except Exception as e:
        logging.error(f"An error occurred in the ingestion process: {e}")
        exit(1)

if __name__ == '__main__':
    main()
