import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


with DAG(
    dag_id='05_pipeline_info_sberbank',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='0 1 * * 5'
) as dag:
    pipeline_info_sberbank = BashOperator(
    task_id = 'pipeline_info_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_info.py -path_type 1 -bank_name sberbank'
)