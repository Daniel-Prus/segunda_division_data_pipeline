import os
from datetime import date
import sqlalchemy as sql
import pyodbc


def df_to_csv_backup(file, file_name, path=f'./backup/', tail_name='', index=False, ext_type='csv'):
    """Write DataFrame to comma-separated values (csv) and send to 'Backup_Folder'

        file (obj): DataFrame object.

        file_name (str): Name of csv file.

        tail_name (str): Optional tail string.

        index (bool): Default False. Write row names (index).
    """
    is_exist = os.path.exists(path)

    if not is_exist:
        os.makedirs(path)

    today = date.today()
    str_today = today.strftime("%Y_%m_%d")

    if bool(tail_name) is True:
        return file.to_csv(f'{path}{file_name}_{str_today}_{tail_name}.{ext_type}', index=index)
    else:
        return file.to_csv(f'{path}{file_name}_{str_today}.{ext_type}', index=index)


def create_engine_msql(login, password, server, database, driver):
    engine = sql.create_engine(f'mssql+pyodbc://{login}:{password}@{server}/{database}?driver={driver}')
    return engine


def pyodbc_connection_and_cursor_msql(connection_str, autocommit=True):
    conn = pyodbc.connect(connection_str, autocommit=autocommit)
    cursor = conn.cursor()
    return conn, cursor
