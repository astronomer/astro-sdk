from abc import ABCMeta

import sqlalchemy

from astro.sql.tables import Table


class Database(metaclass=ABCMeta):
    _create_schema_statement: str = "CREATE SCHEMA IF NOT EXISTS {}"
    _drop_table_statement: str = "DROP TABLE IF EXISTS {}"
    _create_table_statement: str = ""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    @property
    def hook(self):
        raise NotImplementedError

    @property
    def connection(self):
        raise NotImplementedError

    @property
    def sqlalchemy_engine(self):
        return self.hook.get_sqlalchemy_engine()

    def run_sql(self, sql_statement, parameters=None):
        return self.hook.run(sql_statement, parameters)

    # ---------------------------------------------------------
    # Creation & deletion methods
    # ---------------------------------------------------------
    def create_table(self, table: Table) -> None:
        metadata = sqlalchemy.MetaData(
            schema=table.metadata.schema
        )  # mypy: ignore-errors
        sqlalchemy_table = sqlalchemy.Table(
            table.name, metadata, *table.columns
        )  # mypy: ignore-errors
        metadata.create_all(
            self.sqlalchemy_engine, tables=[sqlalchemy_table]
        )  # mypy: ignore-errors

    def drop_table(self, table):
        statement = self._drop_table_statement.format(table.qualified_name)
        self.hook.run(statement)

    # ---------------------------------------------------------
    # Load methods
    # ---------------------------------------------------------
    def load_file_to_table(self, source_file, target_table):
        raise NotImplementedError

    def load_pandas_dataframe_to_table(self, source_dataframe, target_table):
        raise NotImplementedError

    # ---------------------------------------------------------
    # Extract methods
    # ---------------------------------------------------------
    def export_table_to_file(self, source_table, target_file):
        raise NotImplementedError

    def export_table_to_dataframe(self, source_table):
        raise NotImplementedError

    # ---------------------------------------------------------
    # Transformation methods
    # ---------------------------------------------------------
    def select_from_table_to_table(self, source_table, target_table):
        raise NotImplementedError

    def merge_tables(self, source_table, target_table):
        raise NotImplementedError
