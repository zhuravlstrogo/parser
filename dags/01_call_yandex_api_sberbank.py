import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


with DAG(
    dag_id='01_call_yandex_api_sberbank',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='49 16 1 * *'
) as dag:
    call_yandex_api_sberbank = BashOperator(
    task_id = 'call_yandex_api_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/call_yandex_api_org.py -path_type 1 -bank_name sberbank',
)