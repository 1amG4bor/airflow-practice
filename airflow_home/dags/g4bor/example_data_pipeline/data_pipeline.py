from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.edgemodifier import Label
from airflow.providers.postgres.operators.postgres import PostgresOperator

from g4bor.example_data_pipeline.data_ingestion import build_data_ingestion_taskgroup
from g4bor.example_data_pipeline.data_cleaning import build_data_cleaning_taskgroup
from g4bor.example_data_pipeline.data_processing import build_data_processing_taskgroup
from g4bor.operators.const import default_args, user_types


with DAG(
    dag_id='Data_processing_pipeline_MODULARIZED',
    description='Test data processing pipeline',
    schedule_interval='@once',
    start_date=days_ago(1),
    tags=['data-processing', 'CSV', 'pandas'],
    default_args=default_args
) as dag:
    
    Variable.set("need_backup", default_args['local_backup'])

    data_ingestion_taskgroup = build_data_ingestion_taskgroup(dag, default_args, user_types)
    data_cleaning_taskgroup = build_data_cleaning_taskgroup(dag, default_args, user_types)
    data_processing_taskgroup = build_data_processing_taskgroup(dag, default_args)

    task_display_USERS_BRONZE = PostgresOperator(
        task_id='display_USERS_BRONZE',
        sql = 'SELECT * FROM users_bronze;',
        postgres_conn_id=default_args['postgres_conn_id'],
        do_xcom_push=True,
        dag=dag,
    )

    task_display_USERS_SILVER = PostgresOperator(
        task_id='display_USERS_SILVER',
        sql = 'SELECT * FROM users_silver;',
        postgres_conn_id=default_args['postgres_conn_id'],
        do_xcom_push=True,
        dag=dag,
    )

    # Display GOLD tables 
    task_display_users_by_location_GOLD = PostgresOperator(
        task_id='users_by_location_GOLD',
        sql = 'SELECT * FROM users_by_location_gold;',
        postgres_conn_id=default_args['postgres_conn_id'],
        do_xcom_push=True,
        dag=dag,
    )

    task_display_users_by_device_GOLD = PostgresOperator(
        task_id='users_by_device_GOLD',
        sql = 'SELECT * FROM users_by_device_gold;',
        postgres_conn_id=default_args['postgres_conn_id'],
        do_xcom_push=True,
        dag=dag,
    )

    task_display_inactive_users_GOLD = PostgresOperator(
        task_id='inactive_users_GOLD',
        sql = 'SELECT * FROM inactive_users_by_country_and_platform_gold;',
        postgres_conn_id=default_args['postgres_conn_id'],
        do_xcom_push=True,
        dag=dag,
    )

# DATA PIPELINE
(data_ingestion_taskgroup 
    >> Label('Display raw Users data') >> task_display_USERS_BRONZE
    >> Label('Ingested raw data for cleaning') >> data_cleaning_taskgroup
    >> Label('Display cleaned & pre-processed Users data') >> task_display_USERS_SILVER
    >> Label('Pre-processed data to extract business values') >> data_processing_taskgroup
    >> [task_display_users_by_location_GOLD, task_display_users_by_device_GOLD, task_display_inactive_users_GOLD]
)