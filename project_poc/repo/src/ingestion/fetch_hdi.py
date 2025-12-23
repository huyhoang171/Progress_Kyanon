import sys
import requests
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv
import os

# Add project root to sys.path to ensure module imports work
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.logger import logger
from src.utils.config_loader import load_settings, resolve_settings_path
from src.load.gcs_loader import upload_dataframe_to_gcs

# Load environment variables for local testing, respecting existing Docker env vars
load_dotenv(override=False)

# Header configuration for browser emulation
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json",
}


def fetch_hdi_direct_to_gcs() -> None:
    """
    Fetch HDI data from the API and stream it directly to Google Cloud Storage.

    This function bypasses local disk storage by keeping the data in memory (DataFrame)
    and uploading it directly to the GCS bucket defined in settings.yaml.

    Raises:
        requests.exceptions.RequestException: If the API call fails.
        Exception: If GCS upload or other processing errors occur.
    """
    try:
        # Load configuration
        config = load_settings()
        url = config["sources"]["hdi"]["url"]
        bucket_name = config["gcp"]["bucket"]
        gcs_prefix = config["paths"]["raw"]

        logger.info(f"Initiating HDI API call to: {url}")
        
        response = requests.get(url, headers=HEADERS, timeout=30)
        response.raise_for_status()
        
        logger.info("Successfully connected to HDI API.")
        
        # 1. Parse JSON to DataFrame
        data_json = response.json()
        df = pd.DataFrame(data_json)
        logger.info(f"HDI Data fetched successfully. Record count: {len(df)}")
        
        # 2. Stream Upload to GCS
        blob_path = f"{gcs_prefix}/hdi_raw.csv"
        upload_dataframe_to_gcs(bucket_name, blob_path, df)

    except Exception as e:
        logger.exception(f"HDI Ingestion pipeline failed: {e}")
        raise

if __name__ == "__main__":
    fetch_hdi_direct_to_gcs()