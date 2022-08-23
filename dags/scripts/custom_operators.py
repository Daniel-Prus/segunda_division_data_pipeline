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

        for output in self.hook.conn.notices:
            self.log.info(output)


class LoadLeagueTableToPostgres(BaseOperator):
    sql = """INSERT INTO {destination_table} VALUES {rows} 
            ON CONFLICT ON CONSTRAINT {conflict_index} DO {conflict_action};"""

    def __init__(self, postgres_conn_id, database, destination_table, xcom_task_id,
                 xcom_task_id_key, conflict_index, conflict_action, target_fields=None, autocommit=False, *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.autocommit = autocommit
        self.postgres_conn_id = postgres_conn_id
        self.database = database
        self.destination_table = destination_table
        self.xcom_task_id = xcom_task_id
        self.xcom_task_id_key = xcom_task_id_key
        self.target_fields = target_fields
        self.conflict_index = conflict_index
        self.conflict_action = conflict_action

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)

        json_data = context['ti'].xcom_pull(task_ids=self.xcom_task_id, key=self.xcom_task_id_key)
        df = pd.read_json(json_data)

        # values as string much faster than loop !!
        rows = list(df.itertuples(index=False, name=None))
        rows = str(rows)[1:-1]

        self.hook.run(LoadLeagueTableToPostgres.sql.format(
            destination_table=self.destination_table,
            rows=rows,
            conflict_index=self.conflict_index,
            conflict_action=self.conflict_action,
        ),
            self.autocommit
        )
        lala =pd.DataFrame(self.log.info)
        lala.to_csv(f'/opt/airflow/dags/files/output_{self.destination_table}.csv', index=False)
        for output in self.hook.conn.notices:
            self.log.info(output)
