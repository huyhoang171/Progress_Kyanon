"""
HDI Categorization Module

Reads curated HDI-WDI data from GCS and adds HDI_Level categorization
based on hdi_rules specified in settings.yaml configuration.
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


class HDICategorizer:
    """
    Categorizer for HDI levels on curated HDI-WDI data.
    
    Reads curated data from GCS, applies HDI categorization based on settings.yaml,
    and outputs updated data back to GCS storage.
    """
    
    def __init__(self, bucket_name: str = None, input_gcs_path: str = None, output_gcs_path: str = None):
        """
        Initialize HDI Categorizer.
        
        Args:
            bucket_name: Name of the GCS bucket (optional, defaults to config/env)
            input_gcs_path: Path to curated CSV in GCS (optional, defaults to config path)
            output_gcs_path: Path to save categorized CSV in GCS (optional, defaults to config path)
        """
        self.bucket_name = bucket_name
        self.input_gcs_path = input_gcs_path
        self.output_gcs_path = output_gcs_path
        self.curated_df = None
        self.categorized_df = None
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
        Load curated HDI-WDI data from GCS /curated directory.
        
        Returns:
            Curated DataFrame loaded from GCS
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
            curated_dir = paths.get("curated", "curated")
            gcs_path = f"{curated_dir}/hdi_wdi_curated.csv"
        else:
            gcs_path = self.input_gcs_path

        try:
            logger.info(f"Loading curated HDI-WDI data from GCS: gs://{bucket_name}/{gcs_path}")
            
            # Initialize credentials/project
            creds, project_id = self._get_credentials_and_project(self.config)
            storage_client = storage.Client(project=project_id, credentials=creds)
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            
            csv_data = blob.download_as_bytes()
            self.curated_df = pd.read_csv(BytesIO(csv_data))
            
            logger.info(f"Loaded {len(self.curated_df)} rows, {len(self.curated_df.columns)} columns")
            logger.info(f"Columns: {list(self.curated_df.columns)}")
            return self.curated_df
        except Exception:
            logger.exception(f"Failed to load curated data from GCS: gs://{bucket_name}/{gcs_path}")
            raise
    
    def categorize_hdi(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply HDI categorization based on hdi_rules from settings.yaml.
        
        Args:
            df: DataFrame with HDI_Value column
            
        Returns:
            DataFrame with added HDI_Level column
        """
        logger.info("Categorizing HDI levels...")
        
        if self.config is None:
            self.config = self.load_config()
        
        result_df = df.copy()
        
        hdi_rules = self.config.get("hdi_rules", [])
        
        if not hdi_rules:
            logger.warning("No hdi_rules configuration found in settings.yaml")
            result_df['HDI_Level'] = 'Unknown'
            return result_df
        
        if 'HDI_Value' not in result_df.columns:
            logger.error("HDI_Value column not found in dataframe")
            result_df['HDI_Level'] = 'Unknown'
            return result_df
        
        # Sort rules by max_value to ensure proper categorization
        sorted_rules = sorted(hdi_rules, key=lambda x: x.get('max_value', 0))
        
        def categorize_value(hdi_value):
            """Categorize a single HDI value based on rules."""
            if pd.isna(hdi_value):
                return 'Unknown'
            
            for rule in sorted_rules:
                label = rule.get('label', 'Unknown')
                max_val = rule.get('max_value', 0)
                
                if hdi_value < max_val:
                    return label
            
            return 'Unknown'
        
        # Apply categorization
        result_df['HDI_Level'] = result_df['HDI_Value'].apply(categorize_value)
        
        # Log categorization statistics
        category_counts = result_df['HDI_Level'].value_counts()
        logger.info(f"HDI Level distribution:")
        for category, count in category_counts.items():
            logger.info(f"  {category}: {count} rows")
        
        return result_df
    
    def transform(self) -> pd.DataFrame:
        """
        Execute the full HDI categorization pipeline.
        
        Pipeline steps:
        1. Load configuration
        2. Load curated data from GCS
        3. Apply HDI categorization based on hdi_rules
        
        Returns:
            DataFrame with HDI_Level column
        """
        logger.info("=" * 60)
        logger.info("Starting HDI Categorization Pipeline")
        logger.info("=" * 60)
        
        # Load configuration
        if self.config is None:
            self.config = self.load_config()
        
        # Load curated data from GCS
        if self.curated_df is None:
            self.load_from_gcs()
        
        # Categorize HDI levels
        df = self.categorize_hdi(self.curated_df)
        
        self.categorized_df = df
        
        logger.info("=" * 60)
        logger.info("HDI Categorization Complete!")
        logger.info(f"Output: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info("=" * 60)
        
        return df
    
    def save(self, output_path: str = None) -> None:
        """
        Save categorized data to local CSV file.
        
        Args:
            output_path: Path to save the categorized CSV file (optional)
        """
        if self.categorized_df is None:
            raise ValueError("No categorized data available. Run transform() first.")
        
        # If caller provided a custom output_path, respect it
        if output_path is not None:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            self.categorized_df.to_csv(output_file, index=False)
            logger.info(f"Saved categorized data to {output_file}")
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
        
        self.categorized_df.to_csv(out_file_path, index=False)
        logger.info(f"Saved categorized data to {out_file_path}")
    
    def save_to_gcs(self, bucket_name: str = None, gcs_path: str = None) -> None:
        """
        Save categorized data back to Google Cloud Storage (GCS).
        
        Resolves bucket name and GCS path from settings.yaml and environment variables if not provided.
        Overwrites the original curated file with the categorized version.
        
        Args:
            bucket_name: Name of the GCS bucket (optional, defaults to config/env)
            gcs_path: Path within the GCS bucket to save the CSV file (optional, defaults to config path)
        """
        if self.categorized_df is None:
            raise ValueError("No categorized data available. Run transform() first.")

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

            logger.info(f"Uploading categorized data to GCS: gs://{bucket_name}/{gcs_path}")

            csv_data = self.categorized_df.to_csv(index=False)
            blob.upload_from_string(data=csv_data, content_type="text/csv")

            logger.info(f"Categorized data upload to GCS completed successfully. Location: gs://{bucket_name}/{gcs_path}")
        except Exception:
            logger.exception("Failed to upload categorized data to GCS")
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
    # Run HDI categorization pipeline (reads from GCS)
    categorizer = HDICategorizer()
    result = categorizer.transform()
    
    # Save to local (Docker environment)
    categorizer.save()
    
    # Save back to GCS (overwrites the curated file)
    categorizer.save_to_gcs()
