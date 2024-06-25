import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


with DAG(
    dag_id='03_pipeline_links_sberbank',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='56 9 17 * *'
) as dag:
pipeline_links_sberbank = BashOperator(
    task_id = 'pipeline_links_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_links.py -path_type 1 -bank_name sberbank'
)