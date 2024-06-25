import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


with DAG(
    dag_id='02_call_yandex_api_alfa_bank',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='50 13 3 * *'
) as dag:
call_yandex_api_alfa_bank = BashOperator(
    task_id = 'call_yandex_api_alfa_bank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/call_yandex_api_org.py -path_type 1 -bank_name alfa_bank'
)