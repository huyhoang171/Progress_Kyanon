"""
Load Module - Database Loader
Loads calculated metrics into the database
"""
import sqlite3
from datetime import datetime
from typing import Optional


def create_daily_metrics_table(db_path: str = '/opt/airflow/airflow.db'):
    """
    Create the Daily_Metrics table if it doesn't exist.
    
    Args:
        db_path: Path to the SQLite database
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS Daily_Metrics (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric_date DATE NOT NULL,
            average_rating REAL NOT NULL,
            total_places INTEGER,
            places_with_rating INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(metric_date)
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"Daily_Metrics table created/verified in {db_path}")


def insert_or_update_metric(
    average_rating: float,
    metric_date: str = None,
    total_places: int = None,
    places_with_rating: int = None,
    db_path: str = '/opt/airflow/airflow.db'
):
    """
    Insert or update the daily metric in the database.
    
    Args:
        average_rating: The calculated average rating
        metric_date: Date for the metric (defaults to today)
        total_places: Total number of places processed
        places_with_rating: Number of places with valid ratings
        db_path: Path to the SQLite database
    """
    if metric_date is None:
        metric_date = datetime.now().strftime('%Y-%m-%d')
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # First, ensure the table exists
    create_daily_metrics_table(db_path)
    
    # Try to insert, or update if the date already exists
    cursor.execute("""
        INSERT INTO Daily_Metrics (metric_date, average_rating, total_places, places_with_rating)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(metric_date) 
        DO UPDATE SET 
            average_rating = excluded.average_rating,
            total_places = excluded.total_places,
            places_with_rating = excluded.places_with_rating,
            updated_at = CURRENT_TIMESTAMP
    """, (metric_date, average_rating, total_places, places_with_rating))
    
    conn.commit()
    
    # Verify the insert/update
    cursor.execute(
        "SELECT * FROM Daily_Metrics WHERE metric_date = ?", 
        (metric_date,)
    )
    result = cursor.fetchone()
    
    conn.close()
    
    print(f"Successfully inserted/updated metric for {metric_date}: "
          f"average_rating={average_rating}, total_places={total_places}, "
          f"places_with_rating={places_with_rating}")
    print(f"Database record: {result}")
    
    return result


def load_task(**context):
    """
    Airflow task function to load average rating into the database.
    
    Args:
        **context: Airflow context containing task instance
        
    Returns:
        dict: Load operation results
    """
    ti = context['ti']
    execution_date = context['ds']  # Get execution date in YYYY-MM-DD format
    
    # Pull average rating from XCom (pushed by transform task)
    average_rating = ti.xcom_pull(task_ids='transform_rating', key='average_rating')
    
    if average_rating is None:
        error_msg = "No average rating received from transform task"
        print(f"ERROR: {error_msg}")
        return {'success': False, 'error': error_msg}
    
    # Pull additional metadata from transform task
    transform_result = ti.xcom_pull(task_ids='transform_rating', key='return_value')
    total_places = transform_result.get('total_places') if transform_result else None
    places_with_rating = transform_result.get('places_with_rating') if transform_result else None
    
    try:
        # Insert or update the metric in the database
        result = insert_or_update_metric(
            average_rating=average_rating,
            metric_date=execution_date,
            total_places=total_places,
            places_with_rating=places_with_rating
        )
        
        return {
            'success': True,
            'metric_date': execution_date,
            'average_rating': average_rating,
            'total_places': total_places,
            'places_with_rating': places_with_rating,
            'db_record_id': result[0] if result else None
        }
    
    except Exception as e:
        error_msg = f"Failed to load metric into database: {str(e)}"
        print(f"ERROR: {error_msg}")
        return {'success': False, 'error': error_msg}
