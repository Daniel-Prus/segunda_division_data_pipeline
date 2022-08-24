from airflow.models import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup
from airflow.operators.dummy import DummyOperator
# from airflow.models.baseoperator import chain
from scripts.custom_operators import LoadDataToPostgres, LoadLeagueTableToPostgres
from scripts.data_processing import get_api_data_to_csv, get_and_push_data
from scripts.postgres_scripts.postgres_supporter import FootballDB

api_credentials = Variable.get("api_football_beta", deserialize_json=True)
dag_config = Variable.get("seg_div_data_pipeline_config_2022", deserialize_json=True)

API_KEY = api_credentials["api_key"]
API_HOST = api_credentials["api_host"]
LEAGUE_ID = dag_config["league_id"]
SEASON = dag_config["season"]
START_DATE = dag_config["start_date"]
END_DATE = dag_config["end_date"]

# sql query to clear tables according to SEASON and LEAGUE_ID
football_db = FootballDB(season=SEASON, league_id=LEAGUE_ID)
clear_data_football_db = football_db.clear_season_data()

with DAG("seg_div_data_pipeline",
         start_date=datetime.fromisoformat(START_DATE),
         end_date=datetime.fromisoformat(END_DATE) + timedelta(days=7),
         dagrun_timeout=timedelta(seconds=60),
         schedule_interval="0 11 * * 2",
         catchup=False,
         template_searchpath="/opt/airflow/dags/scripts/postgres_scripts/",
         tags=['segdiv']) as dag:

    start = DummyOperator(task_id="start")

    get_api_to_csv = PythonOperator(
        task_id="api_to_csv",
        python_callable=get_api_data_to_csv,
        op_kwargs={
            "key": API_KEY,
            "host": API_HOST,
            "league_id": LEAGUE_ID,
            "season": SEASON
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
                "season": SEASON
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
            LoadLeagueTableToPostgres(
                task_id=f"load_{table}",
                postgres_conn_id="postgres_football_db",
                database="football_db",
                destination_table=f"cal.{table}",
                xcom_task_id="process_api_data.process_and_push_data",
                xcom_task_id_key=table,
                conflict_index=f"PK_cal_{table}",
                conflict_action="NOTHING"
            ) for table in ["league_table", "league_table_home", "league_table_away"]
        ]

        calculate_and_load_draw_series = PostgresOperator(
            task_id="calculate_and_load_draw_series",
            postgres_conn_id="postgres_football_db",
            autocommit=True,
            sql=["football_db/views.sql", "football_db/calculate_draw_series.sql"],
            params={'league_id': LEAGUE_ID,
                    'season': SEASON}

        )

        drop_duplicates = PostgresOperator(
            task_id="drop_duplicates",
            postgres_conn_id="postgres_football_db",
            autocommit=True,
            sql=[football_db.api_results_drop_duplicates,
                 football_db.cal_draw_series_drop_duplicates],
        )

        clear_fooball_db >> [load_results, load_fixtures] >> calculate_and_load_draw_series >> drop_duplicates
        clear_fooball_db.set_downstream(load_league_tables)
        drop_duplicates.set_upstream(load_league_tables)

    load_segunda_divison_dw = PostgresOperator(
        task_id="load_segunda_divison_dw",
        postgres_conn_id="postgres_segunda_division_dw",
        autocommit=True,
        sql="segunda_division_dw/load_segdiv_dw.sql",
        params={'league_id': LEAGUE_ID,
                'season': SEASON}
    )

    finished = DummyOperator(task_id="finished")

    start >> get_api_to_csv >> process_api_data >> load_football_db >> load_segunda_divison_dw >> finished
