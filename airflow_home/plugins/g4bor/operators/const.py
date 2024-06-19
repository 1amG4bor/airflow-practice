import os
from pathlib import Path

root_folder = Path(os.path.abspath(os.curdir))

"""USERS ENTITY COLUMNS: id, first_name, last_name, email, active, ip_address, country, gender, os_platform"""
user_types = {
    'id': 'Int64', 'first_name': 'string', 'last_name': 'string', 'email': 'string', 'active': 'boolean',
    'ip_address': 'string', 'country': 'string', 'gender': 'string', 'os_platform': 'string'
}

"""Default arguments for the data-pipeline"""
default_args = {
    'owner': 'G4bor',
    'raw_data_location': f'{root_folder}/airflow_home/data/users_raw.csv',
    'local_backup': True,
    'backup_folder': f'{root_folder}/airflow_home/data/backup/',
    'sqlite-db-path': f'{root_folder}/airflow_home/database/sqlite.db',
    'sqlite_conn_id': 'local_sqlite',
    'postgres_conn_id': 'local_postgres',
    'postgres_conn': 'postgresql+psycopg2://airflow_user:airflow_pass@localhost/airflow_practice'
}