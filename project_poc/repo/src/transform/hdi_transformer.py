"""
HDI Data Transformer

Transforms raw HDI data from UNDP according to data quality requirements.
Reads raw data from GCS /raw directory.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os
from io import BytesIO

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.logger import logger
from src.utils.config_loader import load_settings, resolve_settings_path


class HDITransformer:
    """
    Transformer for Human Development Index (HDI) data.
    
    Processes raw HDI data from UNDP and outputs cleaned, standardized data
    ready for analysis or merging with other data sources.
    """
    
    def __init__(self, bucket_name: str = None, gcs_path: str = None):
        """
        Initialize HDI Transformer.
        
        Args:
            bucket_name: Name of the GCS bucket (optional, defaults to config/env)
            gcs_path: Path within the GCS bucket for raw HDI data (optional, defaults to config path)
        """
        self.bucket_name = bucket_name
        self.gcs_path = gcs_path
        self.raw_df = None
        self.transformed_df = None
    
    def load(self) -> pd.DataFrame:
        """
        Load raw HDI data from GCS /raw directory.
        
        Returns:
            Raw DataFrame loaded from GCS
        """
        from google.cloud import storage
        
        # Load configuration
        cfg = {}
        config_path = resolve_settings_path()
        if config_path.exists():
            try:
                cfg = load_settings(config_path)
            except Exception:
                logger.warning(f"Failed to read settings.yaml from {config_path}")
        else:
            logger.warning(f"Configuration file not found at: {config_path}")

        # Resolve bucket name: explicit param > config > env var
        bucket_name = self.bucket_name or cfg.get("gcp", {}).get("bucket") or os.getenv("GCS_BUCKET") or os.getenv("GCP_BUCKET_NAME")
        if not bucket_name:
            raise ValueError(
                "GCS bucket name not provided and not found in settings.yaml or environment variables. "
                "Set gcp.bucket in settings.yaml, or GCS_BUCKET/GCP_BUCKET_NAME in .env"
            )

        # Resolve GCS path: explicit param > config path
        if self.gcs_path is None:
            paths = cfg.get("paths", {})
            raw_dir = paths.get("raw", "raw")
            gcs_path = f"{raw_dir}/hdi_raw.csv"
        else:
            gcs_path = self.gcs_path

        try:
            logger.info(f"Loading HDI data from GCS: gs://{bucket_name}/{gcs_path}")
            
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            
            csv_data = blob.download_as_bytes()
            self.raw_df = pd.read_csv(BytesIO(csv_data))
            
            logger.info(f"Loaded {len(self.raw_df)} rows, {len(self.raw_df.columns)} columns")
            return self.raw_df
        except Exception:
            logger.exception(f"Failed to load HDI data from GCS: gs://{bucket_name}/{gcs_path}")
            raise
    
    def extract_fields(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract required fields from raw HDI data.
        
        Pivots the data so each indicator becomes a separate column.
        Output columns: Country_Code, Country_Name, Year, HDI_Value, Life_Expectancy, Education_Years
        
        Args:
            df: Raw HDI DataFrame with indicatorCode as rows
            
        Returns:
            DataFrame with pivoted indicator columns
        """
        logger.info("Extracting HDI fields...")
        
        # Pivot data: each indicatorCode becomes a column
        pivot_df = df.pivot_table(
            index=['countryIsoCode', 'country', 'year'],
            columns='indicatorCode',
            values='value',
            aggfunc='first'
        ).reset_index()
        
        # Map to standardized column names
        extracted = pd.DataFrame({
            'Country_Code': pivot_df['countryIsoCode'],
            'Country_Name': pivot_df['country'],
            'Year': pivot_df['year'],
            'HDI_Value': pivot_df.get('hdi', np.nan),           # Human Development Index
            'Life_Expectancy': pivot_df.get('le', np.nan),      # Life Expectancy at Birth
            'Education_Years': pivot_df.get('mys', np.nan)      # Mean Years of Schooling
        })
        
        logger.info(f"Extracted columns: {list(extracted.columns)}")
        logger.info(f"Rows after pivot: {len(extracted)}")
        
        return extracted
    
    def convert_data_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert all columns to consistent data types.
        
        - Value columns (HDI_Value, Life_Expectancy, Education_Years) -> Float64
        - Year -> Int64 (nullable integer)
        - Country_Code, Country_Name -> string
        
        Args:
            df: DataFrame with extracted fields
            
        Returns:
            DataFrame with converted data types
        """
        logger.info("Converting data types...")
        
        converted = df.copy()
        
        # String columns for categorical data
        converted['Country_Code'] = converted['Country_Code'].astype(str)
        converted['Country_Name'] = converted['Country_Name'].astype(str)
        
        # Year as nullable integer (Int64 supports NA values)
        converted['Year'] = pd.to_numeric(converted['Year'], errors='coerce').astype('Int64')
        
        # Value columns as nullable float (Float64 supports NA values)
        value_columns = ['HDI_Value', 'Life_Expectancy', 'Education_Years']
        for col in value_columns:
            converted[col] = pd.to_numeric(converted[col], errors='coerce').astype('Float64')
        
        logger.info(f"Data types after conversion:\n{converted.dtypes}")
        
        return converted
    
    def handle_missing_values(self, df: pd.DataFrame, strategy: str = 'remove') -> pd.DataFrame:
        """
        Handle NULL/missing values in key analytical columns.
        
        Args:
            df: DataFrame to process
            strategy: 
                - 'remove': Drop rows with any missing values in key columns
                - 'interpolate': Interpolate missing values by country and year
                
        Returns:
            DataFrame with missing values handled
        """
        logger.info(f"Handling missing values (strategy={strategy})...")
        
        # Log missing values before processing
        missing_before = df.isnull().sum()
        total_missing = missing_before.sum()
        logger.info(f"Total missing values before: {total_missing}")
        if total_missing > 0:
            logger.info(f"Missing by column:\n{missing_before[missing_before > 0]}")
        
        processed = df.copy()
        
        # Key analytical columns that should not have missing values
        key_columns = ['Country_Code', 'Year', 'HDI_Value', 'Life_Expectancy', 'Education_Years']
        
        if strategy == 'remove':
            # Remove rows with any missing values in key columns
            rows_before = len(processed)
            processed = processed.dropna(subset=key_columns, how='any')
            rows_removed = rows_before - len(processed)
            logger.info(f"Removed {rows_removed} rows with missing values")
            
        elif strategy == 'interpolate':
            # Interpolate missing values within each country's time series
            value_columns = ['HDI_Value', 'Life_Expectancy', 'Education_Years']
            processed = processed.sort_values(['Country_Code', 'Year'])
            
            for col in value_columns:
                processed[col] = processed.groupby('Country_Code')[col].transform(
                    lambda x: x.interpolate(method='linear', limit_direction='both')
                )
            
            logger.info("Applied linear interpolation by country")
        
        # Log missing values after processing
        missing_after = processed.isnull().sum().sum()
        logger.info(f"Total missing values after: {missing_after}")
        logger.info(f"Final row count: {len(processed)}")
        
        return processed
    
    def normalize_keys(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize join keys for consistency across data sources.
        
        - Country_Code: Convert to UPPERCASE, strip whitespace, remove extra spaces
        - Year: Already handled in convert_data_types as Int64
        
        Args:
            df: DataFrame with keys to normalize
            
        Returns:
            DataFrame with normalized keys
        """
        logger.info("Normalizing join keys...")
        
        normalized = df.copy()
        
        # Normalize Country_Code: uppercase, no leading/trailing spaces, no internal spaces
        normalized['Country_Code'] = (
            normalized['Country_Code']
            .str.upper()
            .str.strip()
            .str.replace(r'\s+', '', regex=True)
        )
        
        # Log sample of normalized keys
        unique_codes = sorted(normalized['Country_Code'].unique())
        logger.info(f"Unique Country_Codes ({len(unique_codes)}): {unique_codes[:10]}...")
        
        year_range = (normalized['Year'].min(), normalized['Year'].max())
        logger.info(f"Year range: {year_range[0]} - {year_range[1]}")
        
        return normalized
    
    def transform(self, missing_strategy: str = 'remove') -> pd.DataFrame:
        """
        Execute the full transformation pipeline.
        
        Pipeline steps:
        1. Load raw data
        2. Extract required fields
        3. Convert data types
        4. Normalize keys (before handling missing to ensure consistent keys)
        5. Handle missing values
        
        Args:
            missing_strategy: Strategy for handling missing values ('remove' or 'interpolate')
            
        Returns:
            Fully transformed DataFrame
        """
        logger.info("=" * 60)
        logger.info("Starting HDI Transformation Pipeline")
        logger.info("=" * 60)
        
        # Load data if not already loaded
        if self.raw_df is None:
            self.load()
        
        # Extract fields
        df = self.extract_fields(self.raw_df)
        
        # Convert data types
        df = self.convert_data_types(df)
        
        # Normalize keys (before handling missing for consistent key format)
        df = self.normalize_keys(df)
        
        # Handle missing values
        df = self.handle_missing_values(df, strategy=missing_strategy)
        
        self.transformed_df = df
        
        logger.info("=" * 60)
        logger.info("HDI Transformation Complete!")
        logger.info(f"Output: {len(df)} rows, {len(df.columns)} columns")
        logger.info(f"Columns: {list(df.columns)}")
        logger.info("=" * 60)
        
        return df
    
    def save(self, output_path: str = "data/cleaned/hdi_cleaned.csv") -> None:
        """
        Save transformed data to CSV file.
        
        Args:
            output_path: Path to save the cleaned CSV file
        """
        if self.transformed_df is None:
            raise ValueError("No transformed data available. Run transform() first.")
        # If caller provided a custom output_path, respect it
        default_path = "data/cleaned/hdi_cleaned.csv"
        if output_path != default_path:
            output_file = Path(output_path)
            output_file.parent.mkdir(parents=True, exist_ok=True)
            self.transformed_df.to_csv(output_file, index=False)
            logger.info(f"Saved transformed data to {output_file}")
            return

        # Otherwise, try to resolve path from settings.yaml
        config_path = resolve_settings_path()

        out_file_path = None
        if config_path.exists():
            try:
                cfg = load_settings(config_path)

                paths = cfg.get("paths", {})
                # Prefer an explicit cleaned_path (absolute), else compose from base_dir + cleaned
                cleaned_path = paths.get("cleaned_path")
                if cleaned_path:
                    out_dir = Path(cleaned_path)
                else:
                    base_dir = paths.get("base_dir")
                    cleaned_dir_name = paths.get("cleaned", "cleaned")
                    if base_dir:
                        out_dir = Path(base_dir) / cleaned_dir_name
                    else:
                        out_dir = Path(__file__).resolve().parents[2] / "data" / cleaned_dir_name

                out_dir.mkdir(parents=True, exist_ok=True)
                out_file_path = out_dir / "hdi_cleaned.csv"
            except Exception:
                logger.exception("Failed to read settings.yaml, falling back to local data/cleaned")
        else:
            logger.warning(f"Configuration file not found at: {config_path}")

        # Fallback to repository-local data/cleaned
        if out_file_path is None:
            out_file_path = Path(__file__).resolve().parents[2] / "data" / "cleaned" / "hdi_cleaned.csv"
            out_file_path.parent.mkdir(parents=True, exist_ok=True)

        self.transformed_df.to_csv(out_file_path, index=False)
        logger.info(f"Saved transformed data to {out_file_path}")

    def save_to_gcs(self, bucket_name: str = None, gcs_path: str = None) -> None:
        """
        Save transformed data to Google Cloud Storage (GCS).
        
        Resolves bucket name and GCS path from settings.yaml and environment variables if not provided.
        
        Args:
            bucket_name: Name of the GCS bucket (optional, defaults to config/env)
            gcs_path: Path within the GCS bucket to save the CSV file (optional, defaults to config path)
        """
        if self.transformed_df is None:
            raise ValueError("No transformed data available. Run transform() first.")

        # Load configuration
        import os
        cfg = {}
        config_path = resolve_settings_path()
        if config_path.exists():
            try:
                cfg = load_settings(config_path)
            except Exception:
                logger.warning(f"Failed to read settings.yaml from {config_path}")
        else:
            logger.warning(f"Configuration file not found at: {config_path}")

        # Resolve bucket name: explicit param > config > env var
        if bucket_name is None:
            bucket_name = cfg.get("gcp", {}).get("bucket") or os.getenv("GCS_BUCKET") or os.getenv("GCP_BUCKET_NAME")
            if not bucket_name:
                raise ValueError(
                    "GCS bucket name not provided and not found in settings.yaml or environment variables. "
                    "Set gcp.bucket in settings.yaml, or GCS_BUCKET/GCP_BUCKET_NAME in .env"
                )

        # Resolve GCS path: explicit param > config path
        if gcs_path is None:
            paths = cfg.get("paths", {})
            cleaned_dir = paths.get("cleaned", "cleaned")
            gcs_path = f"{cleaned_dir}/hdi_cleaned.csv"

        try:
            from google.cloud import storage

            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)

            logger.info(f"Uploading HDI cleaned data to GCS: gs://{bucket_name}/{gcs_path}")

            csv_data = self.transformed_df.to_csv(index=False)
            blob.upload_from_string(data=csv_data, content_type="text/csv")

            logger.info(f"HDI cleaned data upload to GCS completed successfully. Location: gs://{bucket_name}/{gcs_path}")
        except Exception:
            logger.exception("Failed to upload HDI cleaned data to GCS")
            raise

# Entry point for direct execution
if __name__ == "__main__":
    # Run transformation pipeline (reads from GCS)
    transformer = HDITransformer()
    result = transformer.transform(missing_strategy='remove')
    
    # Save to GCS (uses config and env vars automatically)
    transformer.save_to_gcs()

