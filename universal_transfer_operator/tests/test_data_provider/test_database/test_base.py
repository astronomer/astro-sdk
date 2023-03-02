import pathlib

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
