from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator


from g4bor.operators import db_operator


def build_data_processing_taskgroup(dag: DAG, args: dict) -> TaskGroup:

    data_processing_taskgroup = TaskGroup(group_id='data_processing')

    """This task makes available crosstab in Postgres to create pivot tables."""
    task_enable_function_extension = PostgresOperator(
        task_id='enable_function_extension',
        task_group=data_processing_taskgroup,
        sql = 'CREATE EXTENSION IF NOT EXISTS tablefunc;',
        postgres_conn_id=args['postgres_conn_id'],
        dag=dag
    )

    task_users_by_location = PythonOperator(
        task_id='users_by_location_gold',
        task_group=data_processing_taskgroup,
        python_callable=db_operator.create_gold_table_users_distributed_by_location,
        op_kwargs={'connection_str': args['postgres_conn']}
    )

    task_users_by_device = PythonOperator(
        task_id='users_by_device_gold',
        task_group=data_processing_taskgroup,
        python_callable=db_operator.create_gold_table_users_distributed_by_device,
        op_kwargs={'connection_str': args['postgres_conn']}
    )

    task_inactive_users = PythonOperator(
        task_id='inactive_users_gold',
        task_group=data_processing_taskgroup,
        python_callable=db_operator.create_gold_table_inactive_userse,
        op_kwargs={'connection_str': args['postgres_conn']}
    )

    
    task_enable_function_extension >> [task_users_by_location, task_users_by_device, task_inactive_users]

    return data_processing_taskgroup

