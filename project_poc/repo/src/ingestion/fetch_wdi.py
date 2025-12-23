import sys
import requests
import pandas as pd
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv
import os

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.utils.logger import logger
from src.utils.config_loader import load_settings, resolve_settings_path
from src.load.gcs_loader import upload_dataframe_to_gcs

# Load environment variables for local testing
load_dotenv(override=False)


def fetch_wdi_indicator(base_url: str, countries: List[str], indicator_code: str, per_page: int = 5000) -> List[Dict]:
    """
    Fetch WDI data for a specific indicator and list of countries.

    Args:
        base_url (str): The WDI API base endpoint.
        countries (List[str]): List of ISO3 country codes.
        indicator_code (str): The specific WDI indicator ID (e.g., NY.GDP.PCAP.CD).
        per_page (int): Number of records to fetch per page. Defaults to 5000.

    Returns:
        List[Dict]: A list of dictionary records returned by the API.

    Raises:
        Exception: If the API response status code is not 200.
    """
    country_str = ";".join(countries)
    url = f"{base_url}/{country_str}/indicator/{indicator_code}"
    params = {"format": "json", "per_page": per_page}
    
    logger.info(f"Calling WDI API for indicator: {indicator_code}")
    
    response = requests.get(url, params=params, timeout=30)
    
    if response.status_code != 200:
        logger.error(f"Failed to fetch WDI data. Status: {response.status_code}")
        raise Exception(f"WDI API Error: {response.status_code}")
    
    data = response.json()
    
    # WDI API returns a list where index 0 is metadata and index 1 is the records list
    if isinstance(data, list) and len(data) > 1:
        return data[1]
    
    return []

def parse_wdi_records(records: List[Dict], indicator_name: str) -> List[Dict[str, Any]]:
    """
    Parse and standardize raw WDI API records.

    Args:
        records (List[Dict]): Raw records from the API.
        indicator_name (str): A friendly name for the indicator (e.g., 'GDP_Per_Capita').

    Returns:
        List[Dict[str, Any]]: A list of cleaned dictionaries suitable for DataFrame conversion.
    """
    parsed = []
    for record in records:
        # Skip records with missing values
        if record.get("value") is None:
            continue 
        
        parsed.append({
            "Country_Code": record.get("countryiso3code"),
            "Country_Name": record.get("country", {}).get("value"),
            "Year": record.get("date"),
            "Indicator_Code": record.get("indicator", {}).get("id"),
            "Indicator_Name": indicator_name,
            "Value": record.get("value"),
        })
    return parsed

def fetch_wdi_direct_to_gcs() -> None:
    """
    Fetch multiple WDI indicators, merge them, and stream directly to GCS.

    This function handles the orchestration of fetching GDP and FDI data,
    parsing them, and uploading the consolidated dataset to GCS.
    """
    try:
        # Load Config
        config = load_settings()
        base_url = config["sources"]["wdi"]["base_url"]
        countries = config["countries"]["asean_10"]
        indicators = config["sources"]["wdi"]["indicators"]
        per_page = config["sources"]["wdi"].get("per_page", 5000)
        
        bucket_name = config["gcp"]["bucket"]
        gcs_prefix = config["paths"]["raw"]
        
        all_records = []
        
        # 1. Fetch GDP per capita
        logger.info("Fetching GDP per capita data...")
        gdp_records = fetch_wdi_indicator(base_url, countries, indicators["gdp_per_capita"], per_page)
        all_records.extend(parse_wdi_records(gdp_records, "GDP_Per_Capita"))
        
        # 2. Fetch FDI net inflows
        logger.info("Fetching FDI net inflows data...")
        fdi_records = fetch_wdi_indicator(base_url, countries, indicators["fdi_net_inflows"], per_page)
        all_records.extend(parse_wdi_records(fdi_records, "FDI_Net_Inflows"))
        
        if not all_records:
            logger.warning("No WDI records parsed. Aborting upload.")
            return
        
        # 3. Convert to DataFrame and Filter
        df = pd.DataFrame(all_records)
        # Verify filtering for ASEAN 10 countries
        df = df[df["Country_Code"].isin(countries)]
        
        logger.info(f"WDI Data processing complete. Total records: {len(df)}")
        
        # 4. Stream Upload to GCS
        blob_path = f"{gcs_prefix}/wdi_raw.csv"
        upload_dataframe_to_gcs(bucket_name, blob_path, df)

    except Exception as e:
        logger.exception(f"WDI Ingestion pipeline failed: {e}")
        raise

if __name__ == "__main__":
    fetch_wdi_direct_to_gcs()