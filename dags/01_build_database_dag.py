from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from airflow.models.baseoperator import chain
from scripts.create_connection import create_conn_postgres, create_conn_file_path
from scripts.postgres_supporter import FootballDB, SegundaDivisionDW
from scripts.dag_docs.docs import DagDocs

football_db = FootballDB()
segunda_division_dw = SegundaDivisionDW()

with DAG("build_database", start_date=datetime(2022, 1, 1),
         schedule_interval=None, catchup=False, tags=['segdiv']) as dag:
    dag.doc_md = DagDocs().build_database_doc

    # create main postgres connection
    add_postgres_localhost_connection = PythonOperator(
        task_id="add_postgres_localhost_connection",
        python_callable=create_conn_postgres,
        op_kwargs={
            "conn_id": "postgres_localhost",
            "conn_type": "postgres",
            "host": "postgres",
            "login": "airflow",
            "pwd": "airflow",
            "port": 5432,
            "desc": "main_connection"
        }
    )

    with TaskGroup("build_football_db") as build_football_db:
        create_database = PostgresOperator(
            task_id="create_database",
            postgres_conn_id="postgres_localhost",
            autocommit=True,
            sql=[
                "DROP DATABASE IF EXISTS football_db;",
                "CREATE DATABASE football_db;"
            ]
        )

        add_postgres_connection = PythonOperator(
            task_id="add_postgres_connection",
            python_callable=create_conn_postgres,
            op_kwargs={
                "conn_id": "postgres_football_db",
                "conn_type": "postgres",
                "host": "postgres",
                "schema": "football_db",
                "login": "airflow",
                "pwd": "airflow",
                "port": 5432,
                "desc": "football_db_conn"
            }
        )

        build_tables = PostgresOperator(
            task_id="build_tables",
            autocommit=True,
            postgres_conn_id="postgres_football_db",
            sql=[
                football_db.schemas,
                football_db.api_fixtures,
                football_db.api_results,
                football_db.api_season,
                football_db.api_team,
                football_db.api_league,
                football_db.cal_league_table,
                football_db.cal_league_table_home,
                football_db.cal_league_table_away,
                football_db.val_team_market_value,
                football_db.add_indexes
            ]
        )
        create_database >> add_postgres_connection >> build_tables

    with TaskGroup("build_segunda_division_dw") as build_segunda_division_dw:
        create_database = PostgresOperator(
            task_id="create_database",
            autocommit=True,
            postgres_conn_id="postgres_localhost",
            sql=[
                "DROP DATABASE IF EXISTS segunda_division_dw;",
                "CREATE DATABASE segunda_division_dw;"
            ]

        )

        add_postgres_connection = PythonOperator(
            task_id="add_postgres_connection",
            python_callable=create_conn_postgres,
            op_kwargs={
                "conn_id": "postgres_segunda_division_dw",
                "conn_type": "postgres",
                "host": "postgres",
                "schema": "segunda_division_dw",
                "login": "airflow",
                "pwd": "airflow",
                "port": 5432,
                "desc": "segunda_division_dw_conn"
            }
        )

        build_tables = PostgresOperator(
            task_id="build_tables",
            autocommit=True,
            postgres_conn_id="postgres_segunda_division_dw",
            sql=[
                segunda_division_dw.fact_results,
                segunda_division_dw.fact_standings,
                segunda_division_dw.dim_league,
                segunda_division_dw.dim_season,
                segunda_division_dw.dim_team,
                segunda_division_dw.dim_fixtures,
                segunda_division_dw.dim_standings_type,
                segunda_division_dw.add_indexes
            ]
        )

        create_database >> add_postgres_connection >> build_tables

    data_pipeline_file_path_conn = PythonOperator(
        task_id="data_pipeline_csv_file_conn",
        python_callable=create_conn_file_path,
        op_kwargs={
            "conn_id": "data_pipeline_csv_files",
            "conn_type": "fs",
            "login": "airflow",
            "pwd": "airflow",
            "desc": "data_pipeline_csv_file_conn",
            "extra": '{"path":"/opt/airflow/dags/files"}'
        }

    )

    chain(add_postgres_localhost_connection, [build_football_db, build_segunda_division_dw],
          data_pipeline_file_path_conn)
