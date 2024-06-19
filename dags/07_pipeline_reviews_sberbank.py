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
    "start_date": datetime(2024, 5, 28),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=50000)
}


DAG_NAME = '07_pipeline_reviews_sberbank'

dag = DAG(
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['yandex'],
    schedule_interval='21 9 * * 6'
)

pipeline_reviews_sberbank = BashOperator(
    task_id = 'pipeline_reviews_sberbank',
    bash_command='python3 /opt/airflow/scripts/parser/pipeline_review.py -path_type 1 -bank_name sverbank',
    execution_timeout=timedelta(minutes=50000),
    dag=dag
)

pipeline_reviews_sberbank
