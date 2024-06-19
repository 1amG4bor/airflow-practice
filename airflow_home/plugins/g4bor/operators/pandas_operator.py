import os
from typing import List, Any
import pandas as pd
from airflow.models.taskinstance import TaskInstance


def read_raw_csv(ti: TaskInstance, file_path: str, user_types: dict):
    """Read CSV file and store in 'XComs' as JSON object for further processing."""
    if not os.path.exists(file_path):
        raise ValueError('Invalid filepath for reading raw CSV file.')

    raw_usersDF = pd.read_csv(file_path, dtype=user_types).drop_duplicates()
    
    if raw_usersDF is not None:
        print(raw_usersDF)
        ti.xcom_push(key='raw_users_data', value=raw_usersDF.to_json())
    else:
        raise RuntimeError('Error, during Pandas DataFrame creation.')


def backup_users_data(ti: TaskInstance, export_path: str, xcom_key: str, medallion_lvl: str, user_types: dict):
    """Save the data to local store as backup."""
    json_values = ti.xcom_pull(key=xcom_key)
    df = pd.read_json(json_values, dtype=user_types)
    
    os.makedirs(export_path, exist_ok=True)
    df.to_parquet(f'{export_path}/usersDF-{medallion_lvl}.parquet.gzip', compression='gzip')


def remove_null_values(ti: TaskInstance, null_columns: List[str], user_types: dict):
    """Remove rows where the field along the given columns are NULL."""
    json_raw_data = ti.xcom_pull(key='raw_users_data')
    print(json_raw_data)
    clean_usersDF = pd.read_json(json_raw_data, dtype=user_types)

    headers = clean_usersDF.columns
    if (not all([i in headers for i in null_columns])):
        raise ValueError('Invalid column were provided for cleaning the table!')
    
    clean_usersDF = clean_usersDF.dropna(subset=null_columns)
    print(clean_usersDF)

    ti.xcom_push(key='clean_users_data', value=clean_usersDF.to_json())


def fix_null_values(ti: TaskInstance, null_columns: List[str], default_value: Any, user_types: dict):
    """Fix rows where the field along the given columns are NULL with specifing default values."""
    json_cleaned_null_values = ti.xcom_pull(key='clean_users_data')
    print(json_cleaned_null_values)
    fixed_usersDF = pd.read_json(json_cleaned_null_values, dtype=user_types)
    
    headers = fixed_usersDF.columns
    if (not all([i in headers for i in null_columns])):
        raise ValueError('Invalid column were provided for cleaning the table!')
    
    for col_to_fill in null_columns:
        fixed_usersDF[[col_to_fill]] = fixed_usersDF[[col_to_fill]].fillna(value=default_value)
    print(fixed_usersDF)

    ti.xcom_push(key='silver_users_data', value=fixed_usersDF.to_json())
