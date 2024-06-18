from time import sleep

from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python import PythonOperator

def py_print():
    print('Hello from Airflow!')

def py_count():
    for n in range(1, 11):
        print(f'..processing ({n}s)')
        sleep(0.1)

def py_error():
    raise ValueError("Intentional Error!")

with DAG(
    dag_id = 'DAG-03-Multiple_python_functions_workflow',
    description = 'DAG created tasks from multiple Python function.',
    start_date = days_ago(1),
    schedule_interval='@daily',
    tags=['python', 'multi-task'],
) as dag:
    
    task_A = PythonOperator(
        task_id='1_Echo',
        python_callable=py_print
    )

    task_B = PythonOperator(
        task_id='2_Count',
        python_callable=py_count
    )

    task_C = PythonOperator(
        task_id='3_Echo',
        python_callable=py_print
    )

    task_D = PythonOperator(
        task_id='4_Count',
        python_callable=py_print
    )

    task_E = PythonOperator(
        task_id='5_Echo',
        python_callable=py_print
    )

    task_F = PythonOperator(
        task_id='6_Failing',
        python_callable=py_error
    )

    task_G = PythonOperator(
        task_id='7_Echo',
        python_callable=py_print
    )

task_A >> task_B >> [task_C, task_D] >> task_E >> [task_F, task_G]