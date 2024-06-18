from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from dummy_py_scripts import count, full_name, greetings

with DAG(
    dag_id='DAG-04-Multi_python_files_workflow',
    description='DAG with tasks defined in multiple python file.',
    schedule_interval='@once',
    start_date=days_ago(1),
    tags=['python', 'multi-task', 'multi-file'],
) as dag:    
    
    task_1 = PythonOperator(
        task_id='1_echo',
        python_callable=greetings.say_hi,
        op_kwargs={'name': 'G4bor'}
    )

    task_2 = PythonOperator(
        task_id='2_count',
        python_callable=count.count_back,
        op_args=[10]
    )

    task_3 = PythonOperator(
        task_id='3_concat_name',
        python_callable=full_name.concat_names,
        op_kwargs={'firstname': 'G4bor', 'lastname': 'Dev'}
    )

    task_4 = PythonOperator(
        task_id='4_greeting_with_fullname',
        python_callable=greetings.greet_with_fullname
    )

task_1 >> task_2 >> task_3 >> task_4