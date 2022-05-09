import logging
from airflow import settings
from airflow.models import Connection


def create_conn_postgres(conn_id, conn_type, host="", login="", pwd="", port="", desc="", schema="", extra=""):
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      host=host,
                      schema=schema,
                      login=login,
                      password=pwd,
                      port=port,
                      description=desc,
                      extra=extra)
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()

    if str(conn_name) == str(conn.conn_id):
        return logging.warning(f"Connection {conn.conn_id} already exists")
    session.add(conn)
    session.commit()
    session.close()
    # logging.info(Connection.log_info(conn))
    # logging.info(f'Connection {conn_id} is created')
    # return conn


def create_conn_file_path(conn_id, conn_type, login, pwd, extra, desc):
    conn = Connection(conn_id=conn_id,
                      conn_type=conn_type,
                      login=login,
                      password=pwd,
                      extra=extra,
                      description=desc)
    session = settings.Session()
    conn_name = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
    if str(conn_name) == str(conn.conn_id):
        return logging.warning(f"Connection {conn.conn_id} already exists")

    session.add(conn)
    session.commit()
    session.close()
