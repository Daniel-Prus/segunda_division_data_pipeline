from airflow.models import DAG
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
from airflow.operators.python import PythonOperator
from scripts.create_connection import create_conn_file_path
from airflow.operators.bash import BashOperator

csv_files_sensor = ['results', 'fixtures', 'teams', 'seasons', 'leagues', 'league_table', 'league_table_home',
                    'league_table_away', 'team_market_value', 'draw_series']

with DAG("restore_data", start_date=datetime(2022, 1, 1),
         schedule_interval=None, catchup=False, tags=['segdiv']) as dag:
    # create file path connection for csv files
    add_file_path_conn = PythonOperator(
        task_id="add_restore_data_file_path_conn",
        python_callable=create_conn_file_path,
        op_kwargs={
            "conn_id": "restore_data_csv_files",
            "conn_type": "fs",
            "login": "airflow",
            "pwd": "airflow",
            "desc": "csv_files_connection",
            "extra": '{"path":"/opt/airflow/dags/restore_historical_data/files"}'
        }

    )

    remove_csv_files_if_exists = BashOperator(
        task_id="remove_csv_files_if_exists",
        bash_command="""
        cd /opt/airflow/dags/restore_historical_data/files && \
        rm -f raw_data.csv results.csv league_table.csv league_table_home.csv league_table_away.csv fixtures.csv \
        draw_series.csv      
        """,
        do_xcom_push=False
    )

    run_csv_files_py = BashOperator(
        task_id="run_csv_files_py",
        bash_command="""
        cd /opt/airflow/dags/restore_historical_data && \
        python run_segdiv_data.py
        """,
        do_xcom_push=False
    )

    csv_sensor_tasks = [
        FileSensor(
            task_id=f"{csv_file}_csv_sensor",
            fs_conn_id="restore_data_csv_files",
            filepath=f"{csv_file}.csv",
            poke_interval=20,
            timeout=60,
            mode='poke',  # reschedule
            soft_fail=False

        ) for csv_file in csv_files_sensor
    ]

    add_file_path_conn >> remove_csv_files_if_exists >> run_csv_files_py >> csv_sensor_tasks
