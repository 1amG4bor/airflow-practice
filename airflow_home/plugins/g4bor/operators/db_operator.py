import pandas as pd
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.engine.reflection import Inspector
from airflow.models.taskinstance import TaskInstance

from g4bor.operators import sql_query as sql


def save_users_data_to_medallion_table(ti: TaskInstance, connection_str: str, user_types: dict, xcom_key: str, medallion_lvl: str):
    """Save Users data to medallion leveled database table."""
    json_values = ti.xcom_pull(key=xcom_key)
    df = pd.read_json(json_values, dtype=user_types)
    df.set_index('id', inplace=True)    
    
    medallion_lvl = medallion_lvl.lower()
    
    engine = _create_db_engine(connection_str)
    inspector: Inspector = inspect(engine)
    with engine.connect() as conn:
        conn.execute(sql.drop_table_users.format(medallion_lvl))
        conn.execute(sql.create_table_users.format(medallion_lvl))
        if inspector.has_table(f'users_{medallion_lvl}'):
            df.to_sql(f'users_{medallion_lvl}', con=conn, if_exists='replace')
        else:
            raise RuntimeError("Database table couldn't be created!")


def create_gold_table_users_distributed_by_location(connection_str: str):
    with _create_db_engine(connection_str).connect() as conn:
        conn.execute(sql.drop_table_users_by_location_GOLD)
        conn.execute(sql.users_distributed_by_location_GOLD)


def create_gold_table_users_distributed_by_device(connection_str: str):
    with _create_db_engine(connection_str).connect() as conn:
        conn.execute(sql.drop_table_users_by_device_GOLD)
        conn.execute(sql.users_distributed_by_device_GOLD)
        

def create_gold_table_inactive_userse(connection_str: str):
    with _create_db_engine(connection_str).connect() as conn:
        conn.execute(sql.drop_table_inactive_users_GOLD)
        conn.execute(sql.inactive_users_by_country_and_platform_GOLD)


def _create_db_engine(connection_str: str):
    return sa.create_engine(connection_str)
