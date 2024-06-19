from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator, BranchPythonOperator

from g4bor.operators import pandas_operator, db_operator, python_operator

def build_data_cleaning_taskgroup(dag: DAG, args: dict, user_types: dict) -> TaskGroup:
    
    data_cleaning_taskgroup = TaskGroup(group_id='data_cleaning')

    # DATA CLEANING
    task_remove_null_values = PythonOperator(
        task_id='remove_null_values',
        task_group=data_cleaning_taskgroup,
        python_callable=pandas_operator.remove_null_values,
        op_kwargs={'null_columns': ['id', 'email'], 'user_types': user_types}
    )

    task_fix_rows = PythonOperator(
        task_id='fix_null_values',
        task_group=data_cleaning_taskgroup,
        python_callable=pandas_operator.fix_null_values,
        op_kwargs={
            'null_columns': ['active'],
            'default_value': False,
            'user_types': user_types}
    )

    # CREATE 'SILVER' DATASET
    task_if_need_backup_SILVER = BranchPythonOperator(
        task_id='Do_we_need_SILVER_backup',
        task_group=data_cleaning_taskgroup,
        python_callable=python_operator.determine_if_backup_needed,
        op_kwargs={'medallion_lvl': 'SILVER'}
    )

    task_backup_data_SILVER = PythonOperator(
        task_id='data_backup_SILVER',
        task_group=data_cleaning_taskgroup,
        python_callable=pandas_operator.backup_users_data,
        op_kwargs={
            'export_path': args['backup_folder'],
            'xcom_key': 'silver_users_data',
            'medallion_lvl': 'SILVER',
            'user_types': user_types}
    )

    task_create_table_SILVER = PythonOperator(
        task_id='create_table_SILVER',
        task_group=data_cleaning_taskgroup,
        python_callable=db_operator.save_users_data_to_medallion_table,
        do_xcom_push=True,
        op_kwargs={
            'connection_str': args['postgres_conn'],
            'user_types': user_types,
            'xcom_key': 'silver_users_data',
            'medallion_lvl': 'SILVER'}
    )

    # Data cleaning's group workflow
    task_remove_null_values >> task_fix_rows >> task_if_need_backup_SILVER >> [
    task_backup_data_SILVER, task_create_table_SILVER]
    task_backup_data_SILVER >> task_create_table_SILVER

    return data_cleaning_taskgroup