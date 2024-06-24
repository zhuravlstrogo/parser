import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python import PythonOperator

# from scripts.yandex_info_reviews_parser.pipeline_review import pipeline_review_alfa_bank

 
DAG_NAME = '08_pipeline_reviews_alfa_bank'
default_args = {
    "owner": "anyarulina",
    "start_date": datetime(2024, 5, 28),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=50000),
    
}

with DAG(
    # f'{DAG_NAME}',
    dag_id='08_pipeline_reviews_alfa_bank',
    catchup=False,
    start_date=datetime(2024, 6, 24),
    tags=['yandex'],
    # params={"donot_pickle": "True"} ,
    schedule_interval='43 13 * * 4'
) as dag:
    pipeline_reviews_alfa_bank = BashOperator(
    task_id = 'reviews_alfa_bank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_review.py -path_type 1 -bank_name alfa_bank',
    # execution_timeout=timedelta(minutes=50000),
    # dag=dag
)
# (
#     pipeline_reviews_alfa_bank
# )

