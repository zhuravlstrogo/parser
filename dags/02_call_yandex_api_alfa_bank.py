import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

 
default_args = {
    "owner": "anyarulina",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 24),
    "email": ["al.yarulina@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id='02_call_yandex_api_alfa_bank',
    schedule_interval='50 13 3 * *',
    catchup=False,
    default_args=default_args
    # tags=['yandex'],
) as dag:
    call_yandex_api_alfa_bank = BashOperator(
    task_id = 'call_yandex_api_alfa_bank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/call_yandex_api_org.py -path_type 1 -bank_name alfa_bank'
)