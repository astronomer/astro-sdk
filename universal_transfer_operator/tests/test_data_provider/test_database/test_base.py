import pathlib
from unittest import mock

import pandas as pd
import pytest
from pandas import DataFrame

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table

CWD = pathlib.Path(__file__).parent


class DatabaseDataProviderSubclass(DatabaseDataProvider):
    pass


def test_openlineage_database_dataset_namespace():
    """
    Test the open lineage dataset namespace for base class
    """
    db = DatabaseDataProviderSubclass(dataset=Table(name="test"), transfer_mode=TransferMode.NONNATIVE)
    with pytest.raises(NotImplementedError):
        db.openlineage_dataset_namespace()


def test_openlineage_database_dataset_name():
    """
    Test the open lineage dataset names for the base class
    """
    db = DatabaseDataProviderSubclass(dataset=Table(name="test"), transfer_mode=TransferMode.NONNATIVE)
    with pytest.raises(NotImplementedError):
        db.openlineage_dataset_name(table=Table)


def test_subclass_missing_not_implemented_methods_raise_exception():
    db = DatabaseDataProviderSubclass(dataset=Table(name="test"), transfer_mode=TransferMode.NONNATIVE)
    with pytest.raises(NotImplementedError):
        db.hook

    with pytest.raises(NotImplementedError):
        db.sqlalchemy_engine

    with pytest.raises(NotImplementedError):
        db.connection

    with pytest.raises(NotImplementedError):
        db.default_metadata

    with pytest.raises(NotImplementedError):
        db.run_sql("SELECT * FROM inexistent_table")


def test_create_table_using_native_schema_autodetection_not_implemented():
    db = DatabaseDataProviderSubclass(dataset=Table(name="test"), transfer_mode=TransferMode.NONNATIVE)
    with pytest.raises(NotImplementedError):
        db.create_table_using_native_schema_autodetection(table=Table(), file=File(path="s3://bucket/key"))


def test_subclass_missing_load_pandas_dataframe_to_table_raises_exception():
    db = DatabaseDataProviderSubclass(dataset=Table(name="test"), transfer_mode=TransferMode.NONNATIVE)
    table = Table()
    df = DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
    with pytest.raises(NotImplementedError):
        db.load_pandas_dataframe_to_table(df, table)


def test_create_table_using_columns_raises_exception():
    db = DatabaseDataProviderSubclass(dataset=Table(name="test"), transfer_mode=TransferMode.NONNATIVE)
    table = Table()
    with pytest.raises(ValueError) as exc_info:
        db.create_table_using_columns(table)
    assert exc_info.match("To use this method, table.columns must be defined")


@mock.patch(
    "universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.schema_exists",
    return_value=False,
)
@mock.patch("universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.run_sql")
def test_create_schema_if_needed(mock_run_sql, mock_schema_exists):
    """
    Test that run_sql is called with expected arguments when
    create_schema_if_needed method is called when the schema is not available
    """
    dp = DatabaseDataProvider(dataset=Table(name="some_table"), transfer_mode=TransferMode.NONNATIVE)
    dp.create_schema_if_needed("non-existing-schema")
    mock_run_sql.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS non-existing-schema")


@mock.patch(
    "universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.create_schema_if_needed"
)
@mock.patch("universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.drop_table")
@mock.patch("universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.create_table")
@mock.patch("universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.table_exists")
def test_create_schema_if_needed_replace(
    mock_table_exists, mock_create_table, mock_drop_table, mock_create_schema_if_needed
):
    """
    Test that for if_exists == "replace" correct set of methods are called.
    """
    table = Table(name="Some-table")
    df = pd.DataFrame(data={"a": [1, 2], "b": [3, 4]})
    dp = DatabaseDataProvider(dataset=Table(name="some_table"), transfer_mode=TransferMode.NONNATIVE)
    dp.create_schema_and_table_if_needed_from_dataframe(table=table, dataframe=df)
    mock_drop_table.assert_called_once_with(table)
    mock_table_exists.assert_not_called()
    mock_create_table.assert_called_once_with(
        table, dataframe=df, columns_names_capitalization="original", use_native_support=True
    )


@mock.patch(
    "universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.create_schema_if_needed"
)
@mock.patch("universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.drop_table")
@mock.patch("universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.create_table")
@mock.patch(
    "universal_transfer_operator.data_providers.database.base.DatabaseDataProvider.table_exists",
    return_value=False,
)
def test_create_schema_if_needed_append(
    mock_table_exists, mock_create_table, mock_drop_table, mock_create_schema_if_needed
):
    """
    Test that for if_exists == "append" correct set of methods are called.
    """
    table = Table(name="Some-table")
    df = pd.DataFrame(data={"a": [1, 2], "b": [3, 4]})
    dp = DatabaseDataProvider(dataset=Table(name="some_table"), transfer_mode=TransferMode.NONNATIVE)
    dp.create_schema_and_table_if_needed_from_dataframe(table=table, dataframe=df, if_exists="append")
    mock_drop_table.assert_not_called()
    mock_table_exists.assert_called_once_with(table)
    mock_create_table.assert_called_once_with(
        table, dataframe=df, columns_names_capitalization="original", use_native_support=True
    )
