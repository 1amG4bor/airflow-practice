from airflow.models import Variable
from airflow.models.taskinstance import TaskInstance


def determine_if_backup_needed(medallion_lvl: str):
    is_backup_needed = Variable.get('need_backup', default_var=False)

    if medallion_lvl == 'BRONZE':
        return 'data_ingestion.data_backup_BRONZE' if is_backup_needed else 'data_ingestion.create_table_BRONZE'
    if medallion_lvl == 'SILVER':
        return 'data_cleaning.data_backup_SILVER' if is_backup_needed else 'data_cleaning.drop_table_USERS_SILVER'
    return ''

def determine_environment():
    env = Variable.get('environment', default_var='DEV')

    if env == 'PROD':
        return 'PROD_deployment'
    elif env == 'STAGING':
        return 'STAGING_deployment'
    else:
        return 'DEV_deployment'
