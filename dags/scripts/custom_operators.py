from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd


class LoadDataToPostgres(BaseOperator):

    def __init__(self, postgres_conn_id, database, destination_table, xcom_task_id,
                 xcom_task_id_key, target_fields=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.destination_table = destination_table
        self.xcom_task_id = xcom_task_id
        self.xcom_task_id_key = xcom_task_id_key
        self.target_fields = target_fields

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

        json_data = context['ti'].xcom_pull(task_ids=self.xcom_task_id, key=self.xcom_task_id_key)
        df = pd.read_json(json_data)

        rows = list(df.itertuples(index=False, name=None))
        self.hook.insert_rows(table=self.destination_table, rows=rows, target_fields=self.target_fields, commit_every=0)
