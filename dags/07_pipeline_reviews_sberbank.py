import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator


with DAG(
    dag_id='07_pipeline_review_sberbank',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    schedule_interval='21 9 * * 6'
) as dag:
    pipeline_reviews_sberbank = BashOperator(
    task_id = 'pipeline_reviews_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_review.py -path_type 1 -bank_name sberbank'
)