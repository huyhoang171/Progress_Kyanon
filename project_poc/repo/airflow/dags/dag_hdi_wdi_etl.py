from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import time

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
}

def init_task():
    print("Initializing HDI, WDI ingestion DAG")
    time.sleep(10)
    print("Initialization complete")

def end_dag():
    print("HDI, WDI ingestion DAG completed successfully.")

with DAG(
    dag_id='data_ingestion_every_2_days',
    default_args=default_args,
    description='Ingest HDI data for 10 ASEAN countries every 2 days',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 0 */2 * *',  # Every 2 days at midnight
    catchup=False,
    tags=['hdi', 'ingestion', 'asean'],
) as dag:

    init_task = PythonOperator(
        task_id='init_task',
        python_callable=init_task
    )
    
    fetch_hdi_direct_to_gcs_task = BashOperator(
        task_id='fetch_hdi_direct_to_gcs_task',
        bash_command='cd /opt/airflow && python -m src.ingestion.fetch_hdi',
    )
    
    fetch_wdi_direct_to_gcs_task = BashOperator(
        task_id= 'fetch_wdi_direct_to_gcs_task',
        bash_command = 'cd /opt/airflow && python -m src.ingestion.fetch_wdi',
    )

    transform_hdi_task = BashOperator(
        task_id='transform_hdi_data',
        bash_command='cd /opt/airflow && python -m src.transform.hdi_transformer',
    )

    transform_wdi_task = BashOperator(
        task_id='transform_wdi_data',
        bash_command='cd /opt/airflow && python -m src.transform.wdi_transformer',
    )
    
    integration_task = BashOperator(
        task_id="integrate_hdi_wdi_data",
        bash_command="cd /opt/airflow && python -m src.transform.hdi_wdi_integration",
    )

    feature_engineering_ratio_calculation_task = BashOperator(
        task_id="feature_engineering_ratios",
        bash_command="cd /opt/airflow && python -m src.transform.ratio_calculation",
    )
    
    feature_engineering_categorization_task = BashOperator(
        task_id="feature_engineering_categorization",
        bash_command="cd /opt/airflow && python -m src.transform.categorization",
    )
    
    end_dag_task = PythonOperator(
        task_id='end_dag_task',
        python_callable=end_dag,
    )
    
    # DAG ordering: init -> fetch -> transform -> integrate -> feature engineering -> end
    init_task >> [fetch_hdi_direct_to_gcs_task, fetch_wdi_direct_to_gcs_task]

    fetch_hdi_direct_to_gcs_task >> transform_hdi_task
    fetch_wdi_direct_to_gcs_task >> transform_wdi_task

    [transform_hdi_task, transform_wdi_task] >> integration_task >> feature_engineering_ratio_calculation_task >> feature_engineering_categorization_task >> end_dag_task