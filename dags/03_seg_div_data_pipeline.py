from airflow.models import DAG
from datetime import datetime
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
# from airflow.models.baseoperator import chain
from scripts.custom_operators import LoadDataToPostgres
from scripts.data_processing import get_api_data_to_csv, get_and_push_data
from dags.scripts.postgres_scripts.postgres_supporter import FootballDB

api_credentials = Variable.get("api_football_beta", deserialize_json=True)
dag_config = Variable.get("seg_div_data_pipeline_config_2021", deserialize_json=True)

api_key = api_credentials["api_key"]
api_host = api_credentials["api_host"]
league_id = dag_config["league_id"]
season = dag_config["season"]
start_date = dag_config["start_date"]
end_date = dag_config["end_date"]

# sql query to clear tables according to season and league_id
football_db = FootballDB(season=season, league_id=league_id)
clear_data_football_db = football_db.clear_season_data()

with DAG("seg_div_data_pipeline", start_date=datetime.fromisoformat(start_date),
         end_date=datetime.fromisoformat(end_date),
         schedule_interval="0 11 * * 2", catchup=True, template_searchpath="/opt/airflow/dags/scripts/postgres_scripts/",
         tags=['segdiv']) as dag:
    start = DummyOperator(task_id="start")

    get_api_to_csv = PythonOperator(
        task_id="api_to_csv",
        python_callable=get_api_data_to_csv,
        op_kwargs={
            "key": api_key,
            "host": api_host,
            "league_id": league_id,
            "season": season
        }
    )
    with TaskGroup("process_api_data") as process_api_data:
        raw_data_csv_sensor = FileSensor(
            task_id="raw_data_csv_sensor",
            fs_conn_id="data_pipeline_csv_files",
            filepath="raw_data.csv",
            poke_interval=10,
            timeout=40,
            mode='poke',  # reschedule
            soft_fail=False
        )

        process_and_push_data = PythonOperator(
            task_id="process_and_push_data",
            python_callable=get_and_push_data,
            op_kwargs={
                "season": season
            }
        )

        raw_data_csv_sensor >> process_and_push_data

    with TaskGroup("load_football_db", ) as load_football_db:
        clear_fooball_db = PostgresOperator(
            task_id="clear_fooball_db",
            postgres_conn_id="postgres_football_db",
            autocommit=True,
            sql=clear_data_football_db
        )

        load_results = LoadDataToPostgres(
            task_id="load_results",
            postgres_conn_id="postgres_football_db",
            database="football_db",
            destination_table="api.results",
            xcom_task_id="process_api_data.process_and_push_data",
            xcom_task_id_key="results",
            target_fields=football_db.api_results_columns
        )

        load_fixtures = LoadDataToPostgres(
            task_id="load_fixtures",
            postgres_conn_id="postgres_football_db",
            database="football_db",
            destination_table="api.fixtures",
            xcom_task_id="process_api_data.process_and_push_data",
            xcom_task_id_key="fixtures",
        )

        load_league_tables = [
            LoadDataToPostgres(
                task_id=f"load_{table}",
                postgres_conn_id="postgres_football_db",
                database="football_db",
                destination_table=f"cal.{table}",
                xcom_task_id="process_api_data.process_and_push_data",
                xcom_task_id_key=table
            ) for table in ["league_table", "league_table_home", "league_table_away"]
        ]

        drop_duplicates = PostgresOperator(
            task_id="drop_duplicates",
            postgres_conn_id="postgres_football_db",
            autocommit=True,
            sql=football_db.api_results_drop_duplicates,
        )

        clear_fooball_db >> [load_results, load_fixtures]
        clear_fooball_db.set_downstream(load_league_tables)
        drop_duplicates.set_upstream(load_league_tables)
        drop_duplicates << [load_results, load_fixtures]

    load_segunda_divison_dw = PostgresOperator(
        task_id="load_segunda_divison_dw",
        postgres_conn_id="postgres_segunda_division_dw",
        autocommit=True,
        sql="segunda_division_dw/load_segdiv_dw.sql",
        params={'league_id': league_id,
                'season': season}
    )

    finished = DummyOperator(task_id="finished")

    start >> get_api_to_csv >> process_api_data >> load_football_db >> load_segunda_divison_dw >> finished
