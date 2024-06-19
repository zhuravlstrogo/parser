# import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator

# sys.path.append("../src/")
# from src.cpz import replace_cpz
 
DAG_NAME = '01_call_yandex_api_sberbank'
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
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['yandex'],
    schedule_interval='49 16 1 * *'
) as dag:
    call_yandex_api_sberbank = BashOperator(
    task_id = 'call_yandex_api_sberbank',
    bash_command='python3 /opt/airflow/scripts/parser/call_yandex_api_org.py -path_type 1 -bank_name sberbank',
    execution_timeout=timedelta(minutes=1200)
    # dag=dag
)
(
    call_yandex_api_sberbank
)