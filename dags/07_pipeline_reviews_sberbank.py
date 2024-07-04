import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    "owner": "anyarulina",
    "depends_on_past": False,
    "start_date": datetime(2024, 6, 24),
    "email": ["al.yarulina@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='07_pipeline_review_sberbank',
    schedule_interval='43 13 * * 1',
    default_args=default_args, 
    catchup=False
    # start_date=datetime(2024, 6, 24),
    # tags=['yandex'],
    
) as dag:
    pipeline_reviews_sberbank = BashOperator(
    task_id = 'pipeline_reviews_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_review.py -path_type 1 -bank_name sberbank'
)