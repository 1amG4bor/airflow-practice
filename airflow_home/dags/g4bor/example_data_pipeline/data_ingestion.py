from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator

from g4bor.operators import pandas_operator, python_operator, db_operator

def build_data_ingestion_taskgroup(dag: DAG, args: dict, user_types: dict) -> TaskGroup:

    data_ingestion_taskgroup = TaskGroup(group_id='data_ingestion')
    
    # READ THE DATA
    task_read_csv = PythonOperator(
        task_id='read_csv',
        task_group=data_ingestion_taskgroup,
        python_callable=pandas_operator.read_raw_csv,
        op_kwargs={'file_path': args['raw_data_location'], 'user_types': user_types}
    )

    # CREATE 'BRONZE' DATASET
    task_if_need_backup_BRONZE = BranchPythonOperator(
        task_id='Do_we_need_BRONZE_backup',
        task_group=data_ingestion_taskgroup,
        python_callable=python_operator.determine_if_backup_needed,
        op_kwargs={'medallion_lvl': 'BRONZE'}
    )

    task_backup_data_BRONZE = PythonOperator(
        task_id='data_backup_BRONZE',
        task_group=data_ingestion_taskgroup,
        python_callable=pandas_operator.backup_users_data,
        op_kwargs={
            'export_path': args['backup_folder'],
            'xcom_key': 'raw_users_data',
            'medallion_lvl': 'BRONZE',
            'user_types': user_types}
    )

    task_create_table_BRONZE = PythonOperator(
        task_id='create_table_BRONZE',
        task_group=data_ingestion_taskgroup,
        python_callable=db_operator.save_users_data_to_medallion_table,
        do_xcom_push=True,
        op_kwargs={
            'connection_str': args['postgres_conn'],
            'user_types': user_types,
            'xcom_key': 'raw_users_data',
            'medallion_lvl': 'BRONZE'}
    )

    # Data ingestion's group workflow
    task_read_csv >> task_if_need_backup_BRONZE >> [task_backup_data_BRONZE, task_create_table_BRONZE]
    task_backup_data_BRONZE >> task_create_table_BRONZE

    return data_ingestion_taskgroup