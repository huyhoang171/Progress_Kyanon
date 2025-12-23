"""
HDI-WDI Data Integrator

Merges cleaned HDI and WDI datasets into a unified dataset for analysis.
Implements INNER JOIN logic based on Country_Code and Year.
"""

import os
import pandas as pd
import sys
from pathlib import Path
from src.utils.logger import logger
from src.utils.config_loader import load_settings, resolve_settings_path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

try:
    from google.cloud import storage
except Exception:
    storage = None

class HDIWDIIntegrator:
    def __init__(self, hdi_path: str = "data/cleaned/hdi_cleaned.csv", 
                 wdi_path: str = "data/cleaned/wdi_cleaned.csv"):
        self.hdi_path = hdi_path
        self.wdi_path = wdi_path
        self.hdi_df = None
        self.wdi_df = None
        self.merged_df = None
        
    def load_datasets(self) -> None:
        '''
        Load both dataset CSV files into pandas DataFrames.
        '''
        try:
            logger.info("Loading HDI dataset from %s", self.hdi_path)
            self.hdi_df = pd.read_csv(self.hdi_path)
            
            logger.info("Loading WDI dataset from %s", self.wdi_path)
            self.wdi_df = pd.read_csv(self.wdi_path)
        except FileNotFoundError as e:
            logger.error("HDI dataset file not found: %s", e)
            raise
        
    def _prepare_keys(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        '''
        Ensure join keys are of the correct type before merging.
        Args:
            df: DataFrame to process
            source_name: Name of source for logging
            
        Returns:
            DataFrame with standardized keys
        '''
        
        df_copy = df.copy()
        if 'Country_Code' in df_copy.columns:
            df_copy['Country_Code'] = df_copy['Country_Code'].astype(str).str.strip().str.upper()
            
        if 'Year' in df_copy.columns:
            df_copy['Year'] = pd.to_numeric(df_copy['Year'], errors='coerce').astype('Int64')
        
        logger.info("Standardized keys for %s dataset", source_name)
        
        return df_copy
    
    def merge_datasets(self) -> pd.DataFrame:
        '''
        Execute the merge operation.
        Implements:
        - AC4.1: INNER JOIN operation.
        - AC4.2: Join on composite key (Country_Code AND Year).
        - AC4.3: Output contains only matches from both sources.
        Returns:
            Merged DataFrame
        '''
        
        logger.info("=" * 60)
        logger.info("Starting HDI-WDI dataset merge operation")
        logger.info("=" * 60)
        
        if self.hdi_df is None or self.wdi_df is None:
            self.load_datasets()
            
        '''
        Standardize join keys
        '''
        hdi_prep = self._prepare_keys(self.hdi_df, "HDI")
        wdi_prep = self._prepare_keys(self.wdi_df, "WDI")
        
        '''
        Perform INNER JOIN on Country_Code and Year
        '''
        
        if "Country_Name" in wdi_prep.columns:
            wdi_prep = wdi_prep.drop(columns=["Country_Name"])
        
        logger.info("Merging datasets on ['Country_Code', 'Year']")
        merged = pd.merge(
            left=hdi_prep,
            right=wdi_prep,
            how="inner",
            on=["Country_Code", "Year"],
            validate= "1:1"
        )
        
        self.merged_df = merged
        
        total_hdi = len(hdi_prep)
        total_wdi = len(wdi_prep)
        total_merged = len(merged)
        
        logger.info("Total HDI records: %d", total_hdi)
        logger.info("Total WDI records: %d", total_wdi)
        logger.info("Total merged records: %d", total_merged)
        
        if total_merged == 0:
            logger.warning("No matching records found in the merge operation.")
        else:
            logger.info("Merge operation completed successfully. Total records in merged dataset: %d", total_merged)
            
        return merged
    
    def save(self, output_path: str = "/opt/airflow/data/cleaned/hdi_wdi_merged.csv", gcs_bucket: str = None, gcs_blob: str = None, gcs_credentials_path: str = None) -> None:
        '''
        Save the merged DataFrame to a CSV file locally and optionally upload to GCS.
        Args:
            output_path: Path to save the merged CSV file locally
            gcs_bucket: Optional GCS bucket name to upload the CSV
            gcs_blob: Optional destination blob name in the GCS bucket
            gcs_credentials_path: Optional path to GCP service account JSON credentials
        '''
        if self.merged_df is None:
            raise ValueError("No merged data to save.")

        # Create path if it doesn't exist
        output_file = Path(output_path)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        self.merged_df.to_csv(output_path, index=False)
        logger.info("Merged dataset saved to %s", output_path)

        if gcs_bucket:
            try:
                self._upload_to_gcs(gcs_bucket, gcs_blob or output_file.name, gcs_credentials_path)
            except Exception as e:
                logger.error("Failed to upload merged dataset to GCS: %s", e)
                raise

    def _upload_to_gcs(self, bucket_name: str, destination_blob_name: str = None, credentials_json_path: str = None) -> None:
        '''
        Internal helper to upload merged DataFrame to Google Cloud Storage.
        '''
        if storage is None:
            raise RuntimeError("google-cloud-storage package is not available. Please install it in the environment.")

        if self.merged_df is None:
            raise ValueError("No merged data to upload to GCS.")

        # If bucket name not provided, try to read from settings.yaml (Airflow docker path first, then local)
        if not bucket_name:
            config_path = resolve_settings_path()
            if config_path.exists():
                try:
                    cfg = load_settings(config_path)
                    bucket_name = cfg.get("gcp", {}).get("bucket")
                except Exception as e:
                    logger.warning("Unable to read config for GCS bucket from %s: %s", config_path, e)
            else:
                logger.warning("Configuration file not found at: %s", config_path)

        if not bucket_name:
            raise ValueError("GCS bucket name must be provided either as argument or in settings.yaml")

        # Initialize client (use service account JSON if provided)
        if credentials_json_path:
            client = storage.Client.from_service_account_json(credentials_json_path)
        else:
            client = storage.Client()

        bucket = client.bucket(bucket_name)
        dest_name = destination_blob_name or Path("hdi_wdi_merged.csv").name
        blob = bucket.blob(dest_name)

        # Use same approach as fetch_hdi: upload CSV string directly
        csv_str = self.merged_df.to_csv(index=False)
        blob.upload_from_string(data=csv_str, content_type="text/csv")

        logger.info("Uploaded merged dataset to gs://%s/%s", bucket_name, dest_name)
        
if __name__ == "__main__":
    integrator = HDIWDIIntegrator(
        hdi_path="data/cleaned/hdi_cleaned.csv",
        wdi_path="data/cleaned/wdi_cleaned.csv"
    )
    
    try:
        final_df = integrator.merge_datasets()

        # Local path to save
        local_out = "/opt/airflow/data/cleaned/hdi_wdi_merged.csv"

        # Optionally upload to GCS if environment variables provided
        gcs_bucket = os.environ.get("GCS_BUCKET")
        gcs_blob = os.environ.get("GCS_BLOB")
        gcs_credentials = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

        # Ensure blob path targets the 'cleaned/' prefix on GCS by default
        if gcs_blob:
            gcs_blob = gcs_blob.lstrip('/')
        else:
            gcs_blob = f"cleaned/{Path(local_out).name}"

        integrator.save(local_out, gcs_bucket=gcs_bucket, gcs_blob=gcs_blob, gcs_credentials_path=gcs_credentials)

        print("Sample of merged dataset:")
        print(final_df.head())

    except Exception as e:
        logger.error("An error occurred during the integration process: %s", e)