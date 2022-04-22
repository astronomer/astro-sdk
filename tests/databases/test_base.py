import pytest
from pandas import DataFrame

from astro.databases.base import BaseDatabase
from astro.sql.tables import Table


class DatabaseSubclass(BaseDatabase):
    pass


def test_subclass_missing_hook_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    with pytest.raises(NotImplementedError):
        db.hook

    with pytest.raises(NotImplementedError):
        db.sqlalchemy_engine

    with pytest.raises(NotImplementedError):
        db.connection

    with pytest.raises(NotImplementedError):
        db.run_sql("SELECT * FROM inexistent_table")


def test_subclass_missing_get_table_qualified_name_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    with pytest.raises(NotImplementedError):
        db.get_table_qualified_name(table)


def test_subclass_missing_load_file_to_table_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    filepath = "/tmp/filepath.csv"
    with pytest.raises(NotImplementedError):
        db.load_file_to_table(filepath, table)


def test_subclass_missing_load_pandas_dataframe_to_table_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    df = DataFrame()
    with pytest.raises(NotImplementedError):
        db.load_pandas_dataframe_to_table(df, table)


def test_subclass_missing_create_table_from_statement_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    statement = ""
    table = Table()
    with pytest.raises(NotImplementedError):
        db.create_table_from_statement(statement, table)


def test_subclass_missing_append_table_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    source_table = Table()
    target_table = Table()
    with pytest.raises(NotImplementedError):
        db.append_table(source_table, target_table)


def test_subclass_missing_export_table_to_file_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    filepath = "/tmp/filepath.csv"
    with pytest.raises(NotImplementedError):
        db.export_table_to_file(table, filepath)


def test_subclass_missing_export_table_to_pandas_dataframe_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    with pytest.raises(NotImplementedError):
        db.export_table_to_pandas_dataframe(table)
