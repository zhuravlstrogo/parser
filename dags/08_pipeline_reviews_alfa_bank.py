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
    dag_id='08_pipeline_reviews_alfa_bank',
    schedule_interval='43 13 * * 4',
    catchup=False,
    default_args=default_args
    # tags=['yandex'],
    # params={"donot_pickle": "True"} ,
    
) as dag:
    pipeline_reviews_alfa_bank = BashOperator(
    task_id = 'reviews_alfa_bank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_review.py -path_type 1 -bank_name alfa_bank'
    # execution_timeout=timedelta(minutes=50000),
    # dag=dag
)
# (
#     pipeline_reviews_alfa_bank
# )

