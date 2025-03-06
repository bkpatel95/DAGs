from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'simple_dag',
    default_args=default_args,
    description='A simple DAG to learn Airflow basics',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    task_print_date = BashOperator(
        task_id='print_date',
        bash_command='date',
    )

    task_sleep = BashOperator(
        task_id='sleep',
        bash_command='sleep 5',
    )

    task_print_date >> task_sleep
