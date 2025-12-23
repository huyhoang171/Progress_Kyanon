from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_team',
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='asean_economics_etl_manual_test',
    default_args=default_args,
    description='Test chay script Python thu cong',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['asean', 'test'],
) as dag:

    run_external_script = BashOperator(
        task_id='run_hdi_fetch_script',
        bash_command='python /opt/airflow/dags/down_data.py '
    )

    run_external_script