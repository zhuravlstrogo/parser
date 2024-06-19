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


DAG_NAME = '01_call_yandex_api_sberbank'

dag = DAG(
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['yandex'],
    schedule_interval='49 16 1 * *'
)

call_yandex_api_sberbank = BashOperator(
    task_id = 'call_yandex_api_sberbank',
    bash_command='python3 /opt/airflow/scripts/parser/call_yandex_api_org.py -path_type 1 -bank_name sberbank',
    execution_timeout=timedelta(minutes=1200),
    dag=dag
)


call_yandex_api_sberbank
