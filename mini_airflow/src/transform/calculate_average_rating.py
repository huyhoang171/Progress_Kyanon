"""
Transform Module - Calculate Average Rating
Processes place data to calculate overall average rating
"""
from typing import List, Dict, Optional


def calculate_average_rating_from_places(places: List[Dict]) -> Optional[float]:
    """
    Calculate the overall average rating from a list of places.
    
    Args:
        places: List of place dictionaries containing 'totalScore' field
        
    Returns:
        float: Average rating across all places with ratings, or None if no ratings
    """
    if not places:
        print("No places data provided")
        return None
    
    # Filter places with valid ratings
    ratings = [
        place.get('totalScore') 
        for place in places 
        if place.get('totalScore') is not None and isinstance(place.get('totalScore'), (int, float))
    ]
    
    if not ratings:
        print("No valid ratings found in places data")
        return None
    
    average_rating = sum(ratings) / len(ratings)
    print(f"Calculated average rating: {average_rating:.2f} from {len(ratings)} places")
    
    return round(average_rating, 2)


def transform_task(**context):
    """
    Airflow task function to transform place data by calculating average rating.
    
    Args:
        **context: Airflow context containing task instance
        
    Returns:
        dict: Transformation results including average rating
    """
    ti = context['ti']
    
    # Pull places data from XCom (pushed by extract task)
    places = ti.xcom_pull(task_ids='extract_places', key='places_data')
    
    if not places:
        print("No places data received from extract task")
        return {'average_rating': None, 'error': 'No data received'}
    
    # Calculate average rating
    average_rating = calculate_average_rating_from_places(places)
    
    if average_rating is None:
        result = {
            'average_rating': None,
            'total_places': len(places),
            'places_with_rating': 0,
            'error': 'No valid ratings found'
        }
    else:
        result = {
            'average_rating': average_rating,
            'total_places': len(places),
            'places_with_rating': sum(1 for p in places if p.get('totalScore') is not None)
        }
        
        # Push average rating to XCom for the load task
        ti.xcom_push(key='average_rating', value=average_rating)
    
    print(f"Transform complete: {result}")
    
    return result
