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
    dag_id='05_pipeline_info',
    schedule_interval='0 1 * * 6',
    default_args=default_args, 
    catchup=False
    # tags=['yandex'],
) as dag:
    pipeline_info_sberbank = BashOperator(
    task_id = 'pipeline_info_sberbank',
    bash_command='python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_info.py -path_type 1 -bank_name sberbank'
    # dag=dag
)
    pipeline_info_alfa_bank = BashOperator(
    task_id = 'pipeline_info_alfa_bank',
    bash_command = 'python3 /opt/airflow/scripts/yandex_info_reviews_parser/pipeline_info.py -path_type 1 -bank_name alfa_bank'
    # trigger_rule = 'all_success',
    # dag=dag
)
    send_yandex_info = BashOperator(
    task_id = 'send_yandex_info',
    bash_command='python3 /opt/airflow/scripts/mail_sender/send_yndx_info.py'
)


pipeline_info_sberbank >> pipeline_info_alfa_bank >> send_yandex_info