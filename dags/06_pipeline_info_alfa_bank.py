from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import datetime as dt
import os
import sys
import json

default_args = {
    "owner": "anyarulina",
    "start_date": datetime(2024, 5, 28),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=50000)
}


DAG_NAME = '06_pipeline_info_alfa_bank'

dag = DAG(
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['yandex'],
    schedule_interval='13 8 * * 1'
)

pipeline_info_alfa_bank = BashOperator(
    task_id = 'pipeline_info_alfa_bank',
    bash_command='python3 /opt/airflow/scripts/yandex-info-reviews-parser/pipeline_info.py -path_type 1 -bank_name alfa_bank',
    execution_timeout=timedelta(minutes=50000),
    dag=dag
)

pipeline_info_alfa_bank