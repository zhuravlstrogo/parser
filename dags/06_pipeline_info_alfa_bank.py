import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


dag = DAG(
    dag_id='06_pipeline_info_alfa_bank',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='13 8 * * 1'
) as dag:
pipeline_info_alfa_bank = BashOperator(
    task_id = 'pipeline_info_alfa_bank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_info.py -path_type 1 -bank_name alfa_bank'
)