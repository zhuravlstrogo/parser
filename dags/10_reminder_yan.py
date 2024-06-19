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
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow"
}


DAG_NAME = '10_reminder_yan'

dag = DAG(
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['Pointer'],
    schedule_interval='0 7 15 * *'
)

reminder = BashOperator(
    task_id = 'reminder',
    bash_command = 'python3 /opt/airflow/scripts/mail_sender/reminder.py',
    dag=dag
)

reminder
