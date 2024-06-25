import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


dag = DAG(
    dag_id='10_yan_reminder',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='0 7 15 * *'
) as dag:
    reminder = BashOperator(
    task_id = 'reminder',
    bash_command = 'python3 /opt/airflow/scripts/mail_sender/reminder.py'
)