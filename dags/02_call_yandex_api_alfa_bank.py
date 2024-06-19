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
    "start_date": datetime(2024, 5, 27),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=600)
}


DAG_NAME = '02_call_yandex_api_alfa_bank'

dag = DAG(
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['yandex'],
    schedule_interval='50 13 3 * *'
)

call_yandex_api_alfa_bank = BashOperator(
    task_id = 'call_yandex_api_alfa_bank',
    bash_command='python3 /opt/airflow/scripts/parser/call_yandex_api_org.py -path_type 1 -bank_name alfa_bank',
    execution_timeout=timedelta(minutes=1200),
    dag=dag
)

call_yandex_api_alfa_bank
