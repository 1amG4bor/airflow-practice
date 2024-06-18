from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'G4bor',
}

with DAG(
    dag_id='DAG-001-basics',
    description = 'First DAG!',
    default_args = default_args,
    start_date = datetime(year=2024, month=1, day=1),
    schedule_interval = None,
) as dag:

    task_A = BashOperator(
        task_id = 'T001',
        bash_command = 'echo "Greetings!"',
    )

    task_B = BashOperator(
        task_id = 'T002',
        bash_command = f'echo "How is your day, {default_args["owner"]}"',
    )

task_A.set_downstream(task_B)
# task_A >> task_B