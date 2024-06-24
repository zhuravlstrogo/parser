import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

 
DAG_NAME = '07_pipeline_review_sberbank'
default_args = {
    "owner": "anyarulina",
    "start_date": datetime(2024, 5, 28),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=50000)
}

with DAG(
    f'{DAG_NAME}',
    default_args=default_args,
    tags=['yandex'],
    schedule_interval='21 9 * * 6'
) as dag:
    pipeline_reviews_sberbank = BashOperator(
    task_id = 'pipeline_reviews_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_review.py -path_type 1 -bank_name sberbank',
    execution_timeout=timedelta(minutes=50000),
    # dag=dag
)
(
    pipeline_reviews_sberbank
)