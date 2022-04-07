from abc import ABCMeta
from functools import cached_property

import pandas as pd
from airflow.models import DagRun, TaskInstance

from astronew.constants import DEFAULT_CHUNK_SIZE
from astronew.table import Table


class BaseDB(metaclass=ABCMeta):

    # Connection types
    conn_types = []
    max_table_name_size = 63

    def __init__(self, table: Table):
        self.table = table
        self.table_name = self.table.table_name
        self.conn_id = self.table.conn_id
        self.database = self.table.database
        self.schema = self.table.schema
        self.warehouse = self.table.warehouse

    @cached_property
    def hook(self):
        return ...

    @cached_property
    def conn(self):
        return ...

    def get_sqlalchemy_engine(self):
        pass

    def load_file(self, file):
        pass

    def save_file(self, file):
        pass

    def load_pandas_dataframe(
        self,
        pandas_dataframe: pd.DataFrame,
        chunksize: int = DEFAULT_CHUNK_SIZE,
        if_exists: str = "replace",
    ):
        engine = self.hook.get_sqlalchemy_engine()

        self.create_schema_if_needed()
        pandas_dataframe.to_sql(
            self.table_name,
            con=engine,
            if_exists=if_exists,
            chunksize=chunksize,
            method="multi",
            index=False,
        )

    def get_pandas_dataframe(self):
        pass

    def run_sql(self, sql):
        pass

    def generate_table_name(self, context) -> str:
        ti: TaskInstance = context["ti"]
        dag_run: DagRun = ti.get_dagrun()
        table_name = f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}".replace(
            "-", "_"
        ).replace(".", "__")[: self.max_table_name_size]
        if not table_name.isidentifier():
            table_name = f'"{table_name}"'
        return table_name

    def generate_temp_table_name(self, table_name):
        pass

    @property
    def qualified_name(self):
        return f"{self.schema}.{self.table_name}" if self.schema else self.table_name

    def create_table(self):
        pass

    def drop_table(self):
        pass

    def schema_exists(self):
        return False

    def create_schema_query(self, schema_id):
        return f"CREATE SCHEMA IF NOT EXISTS {schema_id}"

    def create_schema_if_needed(self):
        if self.schema and not self.schema_exists():
            self.hook.run(self.create_schema_query(self.schema))

    def identifier_args(self):
        """For Merge"""
        return (self.schema, self.table_name) if self.schema else (self.table_name,)

    def pandas_populate_normalize_config(self, ndjson_normalize_sep):
        """
        Validate pandas json_normalize() parameter for databases, since default params result in
        invalid column name. Default parameter result in the columns name containing '.' char.

        :param ndjson_normalize_sep: separator used to normalize nested ndjson.
            https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
        :type ndjson_normalize_sep: str
        :return: return updated config
        :rtype: `dict`
        """
        normalize_config = {
            "meta_prefix": ndjson_normalize_sep,
            "record_prefix": ndjson_normalize_sep,
            "sep": ndjson_normalize_sep,
        }
        return normalize_config
