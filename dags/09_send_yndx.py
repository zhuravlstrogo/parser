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
    dag_id='09_send_yndx',
    schedule_interval='30 0 * * 1',
    catchup=False,
    default_args=default_args
    # tags=['yandex'],
) as dag:
    mail_sender = BashOperator(
    task_id = 'send_yandex_data',
    bash_command='python3 /opt/airflow/scripts/mail_sender/send_yndx_data.py'
)