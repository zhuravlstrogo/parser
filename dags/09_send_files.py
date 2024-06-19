# Dev Rozhkov A.O.


from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.email_operator import EmailOperator
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
    "retries": 2,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=60)
}


DAG_NAME = '09_send_files'

dag = DAG(
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['Pointer'],
    schedule_interval='30 0 * * 1'
)

five_week = BashOperator(
    task_id = 'five_week',
    bash_command='python3 /opt/airflow/scripts/parser_api/send_email.py',
    execution_timeout=timedelta(minutes=120),
    dag=dag
)

sender_5_mail = BashOperator(
    task_id = 'sender_5_mail',
    bash_command = 'python3 /opt/airflow/scripts/mail_sender/02_sender_mail_week.py',
    trigger_rule = 'all_success',
    dag=dag
)


five_week >> sender_5_mail
