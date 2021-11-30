from typing import Dict, Optional

from airflow.decorators.base import DecoratedOperator
from airflow.hooks.base import BaseHook
from airflow.models import DagRun, TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from astro.utils.load_dataframe import move_dataframe_to_sql


class DataframeToSqlOperator(DecoratedOperator):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        output_table_name="",
        **kwargs,
    ):
        """
        :param to_dataframe: This function allows users to pull the current staging table into a pandas dataframe.

            To use this function, please make sure that your decorated function has a parameter called ``input_df``. This
        parameter will be a pandas.Dataframe that you can modify as needed. Please note that until we implement
        spark and dask dataframes, that you should be mindful as to how large your worker is when pulling large tables.
        :param from_s3: Whether to pull from s3 into current database.

            When set to true, please include a parameter named ``s3_path`` in your TaskFlow function. When calling this
        task, you can specify any s3:// path and Airflow will
        automatically pull that file into your database using Panda's automatic data typing functionality.
        :param from_csv: Whether to pull from a local csv file into current database.

            When set to true, please include a parameter named ``csv_path`` in your TaskFlow function.
        When calling this task, you can specify any local path and Airflow will automatically pull that file into your
        database using Panda's automatic data typing functionality.
        :param kwargs:
        """
        self.conn_id = conn_id
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.output_table_name = output_table_name
        self.kwargs = kwargs or {}

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):
        self.conn_type = BaseHook.get_connection(self.conn_id).conn_type

        output_df = self.python_callable(**self.op_kwargs)

        # Retrieve conn type
        conn_type = BaseHook.get_connection(self.conn_id).conn_type

        # Autogenerate table name
        if not self.output_table_name:
            self.output_table_name = self.create_table_name(context)

        # Write df to target db
        # Note: the `method` argument changes when writing to Snowflake
        move_dataframe_to_sql(
            output_table_name=self.output_table_name,
            conn_id=self.conn_id,
            database=self.database,
            warehouse=self.warehouse,
            schema=self.schema,
            conn_type=conn_type,
            df=output_df,
        )

        return self.output_table_name

    def get_snow_hook(self) -> SnowflakeHook:
        """
        Create and return SnowflakeHook.
        :return: a SnowflakeHook instance.
        :rtype: SnowflakeHook
        """
        return SnowflakeHook(
            snowflake_conn_id=self.conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=None,
            schema=self.schema,
            authenticator=None,
            session_parameters=None,
        )

    def _write_dataframe(self):
        if self.conn_type == "postgres":
            from psycopg2 import sql

            self.hook = PostgresHook(
                postgres_conn_id=self.conn_id, schema=self.database
            )
            query = (
                sql.SQL("SELECT * FROM {input_table}")
                .format(input_table=sql.Identifier(self.table_name))
                .as_string(self.hook.get_conn())
            )
            return self.hook.get_pandas_df(query)

        elif self.conn_type == "snowflake":
            hook = self.get_snow_hook()
            return hook.get_pandas_df(
                "SELECT * FROM IDENTIFIER(%(input_table)s)",
                parameters={"input_table": self.table_name},
            )

    @staticmethod
    def create_table_name(context):
        """Generate output table name."""
        ti: TaskInstance = context["ti"]
        dag_run: DagRun = ti.get_dagrun()
        return f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}"
