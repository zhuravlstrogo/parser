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
    dag_id='03_pipeline_links_sberbank',
    schedule_interval='56 9 17 * *', # каждое 17-ое число
    catchup=False,
    default_args=default_args
    # tags=['yandex'],   
) as dag:
    pipeline_links_sberbank = BashOperator(
    task_id = 'pipeline_links_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_links.py -path_type 1 -bank_name sberbank'
)