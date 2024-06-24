import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator

from scripts.yandex_info_reviews_parser.pipeline_review import pipeline_review_alfa_bank

 
DAG_NAME = '08_pipeline_reviews_alfa_bank'
default_args = {
    "owner": "anyarulina",
    "start_date": datetime(2024, 5, 28),
    "depends_on_past": False,
    "retries": 0,
    "catchup": False,
    "run_as_user": "airflow",
    "dagrun_timeout": timedelta(minutes=50000)
}

# with DAG(
#     f'{DAG_NAME}',
#     default_args=default_args,
#     tags=['yandex'],
#     schedule_interval='43 13 * * 4'
# ) as dag:
#     pipeline_reviews_alfa_bank = BashOperator(
#     task_id = 'pipeline_reviews_alfa_bank',
#     bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_review.py -path_type 1 -bank_name alfa_bank',
#     execution_timeout=timedelta(minutes=50000),
#     # dag=dag
# )
# (
#     pipeline_reviews_alfa_bank
# )


with DAG(
    dag_id='pipeline_reviews_alfa_bank',
    schedule_interval='43 13 * * 4', # UTC time +3 h
    catchup=False,
    start_date=datetime(2024, 5, 12),
    tags=["yandex"],
) as dag:

    print_op = PythonOperator(
        task_id="reviews_alfa_bank", python_callable=pipeline_review_alfa_bank
    )
