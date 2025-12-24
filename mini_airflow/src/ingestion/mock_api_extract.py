"""
Mock API Extract Module
Simulates fetching place data from an API
"""
import json
import os
from typing import List, Dict


def fetch_places_from_api() -> List[Dict]:
    """
    Mock function to simulate API call fetching place data.
    In production, this would call an actual API endpoint.
    For now, it reads from the local JSON file.
    
    Returns:
        List[Dict]: List of place dictionaries with ratings
    """
    # Get the path to the source data file
    current_dir = os.path.dirname(os.path.abspath(__file__))
    data_file = os.path.join(current_dir, '..', '..', 'data', 'source', 'source_data.json')
    
    try:
        with open(data_file, 'r', encoding='utf-8') as f:
            places = json.load(f)
        
        print(f"Successfully fetched {len(places)} places from mock API")
        return places
    
    except FileNotFoundError:
        print(f"Error: Data file not found at {data_file}")
        return []
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        return []
    except Exception as e:
        print(f"Error fetching places: {e}")
        return []


def extract_task(**context):
    """
    Airflow task function to extract place data and push to XCom.
    
    Args:
        **context: Airflow context containing task instance
        
    Returns:
        dict: Summary information about the extraction
    """
    ti = context['ti']
    
    # Fetch places from mock API
    places = fetch_places_from_api()
    
    # Create summary
    summary = {
        'total_places': len(places),
        'places_with_rating': sum(1 for p in places if p.get('totalScore')),
    }
    
    # Push places data to XCom for the next task
    ti.xcom_push(key='places_data', value=places)
    
    print(f"Extract complete: {summary['total_places']} total places, "
          f"{summary['places_with_rating']} with ratings")
    
    return summary
