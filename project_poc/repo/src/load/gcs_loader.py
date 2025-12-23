import pandas as pd
from google.cloud import storage
from typing import Optional
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.logger import logger


def upload_dataframe_to_gcs(
    bucket_name: str,
    blob_path: str,
    dataframe: pd.DataFrame,
    content_type: str = "text/csv"
) -> None:
    """
    Upload a pandas DataFrame to Google Cloud Storage as a CSV file.

    Args:
        bucket_name (str): The GCS bucket name.
        blob_path (str): The full path/filename in the bucket (e.g., 'raw/hdi_raw.csv').
        dataframe (pd.DataFrame): The DataFrame to upload.
        content_type (str): The content type of the file. Defaults to 'text/csv'.

    Raises:
        Exception: If the GCS upload fails.
    """
    try:
        # Initialize GCS Client
        # Authentication is handled automatically via GOOGLE_APPLICATION_CREDENTIALS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        logger.info(f"Streaming data to GCS: gs://{bucket_name}/{blob_path}")

        # Convert DataFrame to CSV string and upload
        blob.upload_from_string(
            data=dataframe.to_csv(index=False),
            content_type=content_type
        )

        logger.info(f"Data upload to GCS completed successfully: gs://{bucket_name}/{blob_path}")

    except Exception as e:
        logger.exception(f"Failed to upload data to GCS: {e}")
        raise
