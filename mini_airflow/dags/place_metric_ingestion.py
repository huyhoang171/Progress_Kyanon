"""
Place Metric Ingestion DAG

This DAG extracts place data from a mock API, calculates the average rating,
and loads it into the Daily_Metrics database table.

Tasks:
1. Extract: Fetch place data from mock API and push to XCom
2. Transform: Calculate average rating and push to XCom
3. Load: Insert/update average rating in database
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Add src directory to Python path so we can import our modules
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ingestion.mock_api_extract import extract_task
from transform.calculate_average_rating import transform_task
from load.db_loader import load_task


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
with DAG(
    dag_id='place_metric_ingestion',
    default_args=default_args,
    description='Ingest place metrics: Extract, Transform, and Load average ratings',
    schedule='@daily',  # Run once per day (changed from schedule_interval in Airflow 3.x)
    start_date=datetime(2025, 12, 24),
    catchup=False,
    tags=['places', 'metrics', 'etl'],
) as dag:
    
    # Task 1: Extract places from mock API
    extract_places = PythonOperator(
        task_id='extract_places',
        python_callable=extract_task,
        doc_md="""
        ### Extract Task
        Fetches place data from the mock API (reads from JSON file).
        Pushes the complete list of places to XCom with key 'places_data'.
        
        **Output**: Returns summary with total places and places with ratings.
        """
    )
    
    # Task 2: Transform - Calculate average rating
    transform_rating = PythonOperator(
        task_id='transform_rating',
        python_callable=transform_task,
        doc_md="""
        ### Transform Task
        Pulls place data from XCom, calculates the overall average rating.
        Pushes the calculated average rating to XCom with key 'average_rating'.
        
        **Input**: Places data from extract_places task
        **Output**: Returns transformation results including average rating
        """
    )
    
    # Task 3: Load - Insert into database
    load_to_db = PythonOperator(
        task_id='load_to_db',
        python_callable=load_task,
        doc_md="""
        ### Load Task
        Pulls average rating from XCom and inserts/updates it in the Daily_Metrics table.
        Uses SQLite database (Airflow's default metadata database).
        
        **Input**: Average rating from transform_rating task
        **Output**: Returns load operation results including success status
        """
    )
    
    # Define task dependencies (sequential execution)
    extract_places >> transform_rating >> load_to_db
