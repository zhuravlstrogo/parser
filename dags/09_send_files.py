import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


dag = DAG(
    dag_id='09_send_files',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='30 0 * * 1'
) as dag:
    mail_sender = BashOperator(
    task_id = 'mail_sender',
    bash_command='python3 /opt/airflow/scripts/parser_api/send_email.py'
)