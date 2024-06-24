# import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

# sys.path.append("../src/")
# from src.cpz import replace_cpz
 
DAG_NAME = '05_stop_process'
default_args = {
    "owner": "anyarulina",
    "start_date": datetime(2024, 5, 28),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=50000)
}

with DAG(
    dag_id='05_stop_process',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='0 1 * * 5'
) as dag:
    stop_process = BashOperator(
    task_id = 'stop_process',
    bash_command='/opt/airflow/stop_process.sh ',
    # execution_timeout=timedelta(minutes=50000),
    # dag=dag
)
# (
#     stop_process
# )