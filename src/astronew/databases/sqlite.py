from functools import cached_property

import pandas as pd
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy.engine import Engine, create_engine

from ..constants import DEFAULT_CHUNK_SIZE
from .base import BaseDB


class Sqlite(BaseDB):

    # Connection types
    conn_types = ["sqlite"]

    @cached_property
    def hook(self):
        _hook = SqliteHook(sqlite_conn_id=self.conn_id)
        if self.database:
            _hook.database = self.database
        return _hook

    @property
    def qualified_name(self):
        return self.table_name

    def get_sqlalchemy_engine(self) -> Engine:
        """
        Given a hook, return a SQLAlchemy engine for the target database.

        :return: SQLAlchemy engine
        :rtype: sqlalchemy.Engine
        """
        uri = self.hook.get_uri()
        if "////" not in uri:
            uri = uri.replace("///", "////")
        engine = create_engine(uri)
        return engine

    def load_pandas_dataframe(
        self,
        pandas_dataframe: pd.DataFrame,
        chunksize: int = DEFAULT_CHUNK_SIZE,
        if_exists: str = "replace",
    ):
        engine = self.get_sqlalchemy_engine()
        pandas_dataframe.to_sql(
            self.table_name,
            con=engine,
            if_exists=if_exists,
            chunksize=chunksize,
            method="multi",
            index=False,
        )

    def create_default_schema_if_needed(self):
        """SQLite does not support schemas."""
        pass

    def create_schema_if_needed(self):
        """SQLite does not support schemas."""
        pass
