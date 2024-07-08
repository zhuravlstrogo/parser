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
    dag_id='10_pointer',
    schedule_interval='30 4 * * 1',
    catchup=False,
    default_args=default_args
    # tags=['yandex'],
) as dag:
    pointer_api = BashOperator(
    task_id = 'pointer_api', 
    bash_command='python3 /opt/airflow/scripts/pointer_api/parser.py -path_type 1 > /opt/airflow/scripts/pointer_api/log_parser.txt',
    execution_timeout=timedelta(minutes=120),
    dag=dag
)
    send_pointer_data = BashOperator(
    task_id = 'send_pointer_data',
    bash_command = 'python3 /opt/airflow/scripts/mail_sender/send_pointer_data.py',
    trigger_rule = 'all_success',
    dag=dag
)


pointer_api >> send_pointer_data