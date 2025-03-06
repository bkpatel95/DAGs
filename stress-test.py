from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@task
def compute_fibonacci(n: int) -> int:
    """
    Compute Fibonacci number iteratively.
    The higher n is, the more CPU intensive the task becomes.
    """
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a

with DAG(
    'stress_test_dag',
    default_args=default_args,
    description='A complicated DAG to stress test Airflow',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    start = BashOperator(
        task_id='start',
        bash_command='echo "Starting stress test DAG"',
    )

    # Create several groups, each with multiple dynamically mapped Fibonacci tasks.
    groups = []
    for group_id in range(5):
        with TaskGroup(group_id=f'group_{group_id}') as tg:
            # Here, we create 10 tasks per group that all compute fibonacci(35)
            # Dynamic mapping will create 10 parallel tasks in this group.
            fibonacci_results = compute_fibonacci.expand(n=[35] * 10)
        groups.append(tg)

    finish = BashOperator(
        task_id='finish',
        bash_command='echo "Stress test DAG completed!"',
    )

    start >> groups >> finish
