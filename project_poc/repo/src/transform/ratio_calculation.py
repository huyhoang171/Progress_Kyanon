"""
Ratio Calculation Module

Reads merged HDI-WDI data from GCS and calculates feature engineering ratios
as specified in settings.yaml configuration.
"""
import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from io import BytesIO
from typing import Optional, Tuple

from dotenv import load_dotenv

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.logger import logger
from src.utils.config_loader import load_settings, resolve_settings_path


class RatioCalculator:
    """
    Calculator for feature engineering ratios on merged HDI-WDI data.
    
    Reads merged data from GCS, applies ratio calculations based on settings.yaml,
    and outputs curated data to both local and GCS storage.
    """
    
    def __init__(self, bucket_name: str = None, input_gcs_path: str = None, output_gcs_path: str = None):
        """
        Initialize Ratio Calculator.
        
        Args:
            bucket_name: Name of the GCS bucket (optional, defaults to config/env)
            input_gcs_path: Path to merged HDI-WDI CSV in GCS (optional, defaults to config path)
            output_gcs_path: Path to save curated CSV in GCS (optional, defaults to config path)
        """
        self.bucket_name = bucket_name
        self.input_gcs_path = input_gcs_path
        self.output_gcs_path = output_gcs_path
        self.merged_df = None
        self.curated_df = None
        self.config = None
    
    def load_config(self) -> dict:
        """
        Load configuration from settings.yaml.
        
        Returns:
            Configuration dictionary
        """
        config_path = resolve_settings_path()

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found at {config_path}")

        try:
            cfg = load_settings(config_path)
            logger.info(f"Loaded configuration from {config_path}")
            return cfg
        except Exception:
            logger.exception(f"Failed to read settings.yaml from {config_path}")
            raise
    
    def load_from_gcs(self) -> pd.DataFrame:
        """
        Load merged HDI-WDI data from GCS /cleaned directory.
        
        Returns:
            Merged DataFrame loaded from GCS
        """
        from google.cloud import storage
        
        # Load configuration
        if self.config is None:
            self.config = self.load_config()

        # Resolve bucket name: explicit param > config > env var
        bucket_name = self.bucket_name or self.config.get("gcp", {}).get("bucket") or os.getenv("GCS_BUCKET") or os.getenv("GCP_BUCKET_NAME")
        if not bucket_name:
            raise ValueError(
                "GCS bucket name not provided and not found in settings.yaml or environment variables. "
                "Set gcp.bucket in settings.yaml, or GCS_BUCKET/GCP_BUCKET_NAME in .env"
            )

        # Resolve input GCS path: explicit param > config path
        if self.input_gcs_path is None:
            paths = self.config.get("paths", {})
            cleaned_dir = paths.get("cleaned", "cleaned")
            gcs_path = f"{cleaned_dir}/hdi_wdi_merged.csv"
        else:
            gcs_path = self.input_gcs_path

        try:
            logger.info(f"Loading merged HDI-WDI data from GCS: gs://{bucket_name}/{gcs_path}")
            
            # Initialize credentials/project
            creds, project_id = self._get_credentials_and_project(self.config)
            storage_client = storage.Client(project=project_id, credentials=creds)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            
            csv_data = blob.download_as_bytes()
            self.merged_df = pd.read_csv(BytesIO(csv_data))
            
            logger.info(f"Loaded {len(self.merged_df)} rows, {len(self.merged_df.columns)} columns")
            logger.info(f"Columns: {list(self.merged_df.columns)}")
            return self.merged_df
        except Exception:
            logger.exception(f"Failed to load merged data from GCS: gs://{bucket_name}/{gcs_path}")
            raise
    
    def calculate_ratios(self, df: pd.DataFrame) -> pd.DataFrame:
        logger.info("Calculating feature engineering ratios...")
        
        if self.config is None:
            self.config = self.load_config()
        
        result_df = df.copy()
        
        feature_configs = self.config.get("feature_engineering", [])
        
        if not feature_configs:
            logger.warning("No feature_engineering configuration found")
            return result_df
        
        for feature_config in feature_configs:
            target_col = feature_config.get("target_col")
            formula = feature_config.get("formula")
            
            if not target_col or not formula:
                continue
            
            logger.info(f"Calculating {target_col} = {formula}")
            
            try:
                
                result_series = result_df.eval(formula, engine='python')
                
                result_series = result_series.replace([np.inf, -np.inf], np.nan)
                
                result_df[target_col] = result_series
                
                valid_count = result_df[target_col].notna().sum()
                logger.info(f"Created column '{target_col}' with {valid_count} valid values")
                
            except Exception as e:
                logger.error(f"Failed to calculate {target_col}. Formula: {formula}. Error: {str(e)}")
                result_df[target_col] = np.nan
        
        return result_df
    
    def transform(self) -> pd.DataFrame:
        """
        Execute the full ratio calculation pipeline.
        
        Pipeline steps:
        1. Load configuration
        2. Load merged data from GCS
        3. Calculate ratios based on config
        
        Returns:
            DataFrame with calculated ratios
        """
        logger.info("=" * 60)
        logger.info("Starting Ratio Calculation Pipeline")
        logger.info("=" * 60)
        
        # Load configuration
        if self.config is None:
            self.config = self.load_config()
        
        # Load merged data from GCS
        if self.merged_df is None:
            self.load_from_gcs()
        
        # Calculate ratios
        df = self.calculate_ratios(self.merged_df)
        
        self.curated_df = df
        
        logger.info("=" * 60)
        logger.info("Ratio Calculation Complete!")
        logger.info(f"Output: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info("=" * 60)
        
        return df
    
    def save(self, output_path: str = None) -> None:
        """
        Save curated data to local CSV file.
        
        Args:
            output_path: Path to save the curated CSV file (optional)
        """
        if self.curated_df is None:
            raise ValueError("No curated data available. Run transform() first.")
        
        # If caller provided a custom output_path, respect it
        if output_path is not None:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            self.curated_df.to_csv(output_file, index=False)
            logger.info(f"Saved curated data to {output_file}")
            return

        # Otherwise, try to resolve path from settings.yaml
        if self.config is None:
            self.config = self.load_config()

        paths = self.config.get("paths", {})
        
        # Prefer an explicit curated_path (absolute), else compose from base_dir + curated
        curated_path = paths.get("curated_path")
        if curated_path:
            out_file_path = Path(curated_path) / "hdi_wdi_curated.csv"
        else:
            base_dir = paths.get("base_dir", "/opt/airflow/data")
            curated_dir = paths.get("curated", "curated")
            out_file_path = Path(base_dir) / curated_dir / "hdi_wdi_curated.csv"
        
        # Create directory if it doesn't exist
        out_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        self.curated_df.to_csv(out_file_path, index=False)
        logger.info(f"Saved curated data to {out_file_path}")

    def save_to_gcs(self, bucket_name: str = None, gcs_path: str = None) -> None:
        """
        Save curated data to Google Cloud Storage (GCS).
        
        Resolves bucket name and GCS path from settings.yaml and environment variables if not provided.
        
        Args:
            bucket_name: Name of the GCS bucket (optional, defaults to config/env)
            gcs_path: Path within the GCS bucket to save the CSV file (optional, defaults to config path)
        """
        if self.curated_df is None:
            raise ValueError("No curated data available. Run transform() first.")

        # Load configuration
        if self.config is None:
            self.config = self.load_config()

        # Resolve bucket name: explicit param > config > env var
        if bucket_name is None:
            bucket_name = self.config.get("gcp", {}).get("bucket") or os.getenv("GCS_BUCKET") or os.getenv("GCP_BUCKET_NAME")
            if not bucket_name:
                raise ValueError(
                    "GCS bucket name not provided and not found in settings.yaml or environment variables. "
                    "Set gcp.bucket in settings.yaml, or GCS_BUCKET/GCP_BUCKET_NAME in .env"
                )

        # Resolve output GCS path: explicit param > provided param > config path
        if gcs_path is None:
            if self.output_gcs_path is not None:
                gcs_path = self.output_gcs_path
            else:
                paths = self.config.get("paths", {})
                curated_dir = paths.get("curated", "curated")
                gcs_path = f"{curated_dir}/hdi_wdi_curated.csv"

        try:
            from google.cloud import storage

            # Initialize credentials/project
            creds, project_id = self._get_credentials_and_project(self.config)
            storage_client = storage.Client(project=project_id, credentials=creds)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)

            logger.info(f"Uploading curated data to GCS: gs://{bucket_name}/{gcs_path}")

            csv_data = self.curated_df.to_csv(index=False)
            blob.upload_from_string(data=csv_data, content_type="text/csv")

            logger.info(f"Curated data upload to GCS completed successfully. Location: gs://{bucket_name}/{gcs_path}")
        except Exception:
            logger.exception("Failed to upload curated data to GCS")
            raise

    def _get_credentials_and_project(self, cfg: dict) -> Tuple[Optional[object], Optional[str]]:
        """
        Resolve Google Cloud credentials and project id for local or Docker runs.
        """
        from google.oauth2 import service_account

        # Ensure environment variables from .env are available in local runs
        try:
            load_dotenv(override=False)
        except Exception:
            pass

        project_id = os.getenv("GCP_PROJECT_ID") or (cfg.get("gcp", {}) or {}).get("project_id")

        creds_path_env = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        creds_path: Optional[Path] = None

        if creds_path_env:
            p = Path(creds_path_env)
            if p.exists():
                creds_path = p
            else:
                candidate = Path(__file__).resolve().parents[2] / "secrets" / p.name
                if candidate.exists():
                    creds_path = candidate

        # if creds_path is None:
        #     fallback = Path(__file__).resolve().parents[2] / "secrets" / "chapter-talos-5a3322966f92.json"
        #     if fallback.exists():
        #         creds_path = fallback

        if creds_path and creds_path.exists():
            try:
                creds = service_account.Credentials.from_service_account_file(str(creds_path))
                logger.info(f"Using service account credentials from {creds_path}")
                return creds, project_id
            except Exception:
                logger.warning("Failed to load service account credentials; will fall back to ADC if available")

        return None, project_id


# Entry point for direct execution
if __name__ == "__main__":
    # Run ratio calculation pipeline (reads from GCS)
    calculator = RatioCalculator()
    result = calculator.transform()
    
    # Save to local (Docker environment)
    calculator.save()
    
    # Save to GCS
    calculator.save_to_gcs()
