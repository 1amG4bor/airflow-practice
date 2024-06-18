import os
from pathlib import Path

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.bash import BashOperator

root_folder = Path(os.path.abspath(os.curdir)).parent
scripts_path = root_folder.joinpath('plugins', 'dummy_bash_scripts')

with DAG(
    dag_id = 'DAG-02-Multi_bash_files_workflow',
    description = 'DAG created tasks from multiple bash files.',
    start_date = days_ago(1),
    schedule_interval='@daily',
    tags=['bash', 'multi-task', 'multi-file'],
    template_searchpath=str(scripts_path)
) as dag:
    
    task_A = BashOperator(
        task_id='1_Echo',
        bash_command='echo.sh'
    )

    task_B = BashOperator(
        task_id='2_Count',
        bash_command='count.sh'
    )

    task_C = BashOperator(
        task_id='3_Echo',
        bash_command='echo.sh'
    )

    task_D = BashOperator(
        task_id='4_Count',
        bash_command='echo.sh'
    )

    task_E = BashOperator(
        task_id='5_Echo',
        bash_command='echo.sh'
    )

    task_F = BashOperator(
        task_id='6_Failing',
        bash_command='failing.sh'
    )

    task_G = BashOperator(
        task_id='7_Echo',
        bash_command='echo.sh'
    )

task_A >> task_B >> [task_C, task_D] >> task_E >> [task_F, task_G]