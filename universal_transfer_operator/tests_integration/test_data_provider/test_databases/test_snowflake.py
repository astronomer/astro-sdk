"""Integration Tests specific to the Snowflake Database implementation."""
import pathlib
from pathlib import Path

import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.base import DataStream
from universal_transfer_operator.data_providers.database.snowflake import SnowflakeDataProvider
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.settings import SNOWFLAKE_SCHEMA

DEFAULT_CONN_ID = "snowflake_default"
CUSTOM_CONN_ID = "snowflake_conn"
SUPPORTED_CONN_IDS = [CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
def test_snowflake_run_sql():
    """Test run_sql against snowflake database"""
    statement = "SELECT 1 + 1;"
    dp = SnowflakeDataProvider(
        dataset=Table(name="some_table", conn_id="snowflake_conn"), transfer_mode=TransferMode.NONNATIVE
    )
    response = dp.run_sql(statement, handler=lambda x: x.first())
    assert response[0] == 2


@pytest.mark.integration
def test_table_exists_raises_exception():
    """Test if table exists in snowflake database"""
    database = SnowflakeDataProvider(
        Table(name="inexistent-table", metadata=Metadata(schema=SNOWFLAKE_SCHEMA), conn_id=CUSTOM_CONN_ID),
        transfer_mode=TransferMode.NONNATIVE,
    )
    table = Table(name="inexistent-table", metadata=Metadata(schema=SNOWFLAKE_SCHEMA))
    assert not database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "dataset_table_fixture",
    [
        {
            "dataset": "SnowflakeDataProvider",
            "table": Table(
                metadata=Metadata(schema=SNOWFLAKE_SCHEMA),
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.String(60), nullable=False, key="name"),
                ],
            ),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_snowflake_create_table_with_columns(dataset_table_fixture):
    """Test table creation with columns data"""
    dp, table = dataset_table_fixture

    statement = f"DESC TABLE {dp.get_table_qualified_name(table)}"
    with pytest.raises(ProgrammingError) as e:
        dp.run_sql(statement)
    assert e.match("does not exist or not authorized")

    dp.create_table(table)
    response = dp.run_sql(statement, handler=lambda x: x.fetchall())
    rows = response
    assert len(rows) == 2
    assert rows[0] == (
        "ID",
        "NUMBER(38,0)",
        "COLUMN",
        "N",
        "IDENTITY START 1 INCREMENT 1",
        "Y",
        "N",
        None,
        None,
        None,
        None,
    )
    assert rows[1] == (
        "NAME",
        "VARCHAR(60)",
        "COLUMN",
        "N",
        None,
        "N",
        "N",
        None,
        None,
        None,
        None,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "dataset_table_fixture",
    [
        {
            "dataset": "SnowflakeDataProvider",
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_load_pandas_dataframe_to_table(dataset_table_fixture):
    """Test load_pandas_dataframe_to_table against snowflake"""
    dp, table = dataset_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    dp.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT * FROM {dp.get_table_qualified_name(table)}"
    response = dp.run_sql(statement, handler=lambda x: x.fetchall())

    rows = response
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)


@pytest.mark.integration
@pytest.mark.parametrize(
    "dataset_table_fixture",
    [
        {
            "dataset": "SnowflakeDataProvider",
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_write_method(dataset_table_fixture):
    """Test write() for snowflake"""
    dp, table = dataset_table_fixture
    file_path = f"{str(CWD)}/../../data/sample.csv"
    fs = DataStream(
        remote_obj_buffer=file_path,
        actual_file=File(
            path=file_path,
        ),
        actual_filename=Path(file_path),
    )
    dp.write(source_ref=fs)
    rows = dp.fetch_all_rows(table=dp.dataset)
    rows.sort(key=lambda x: x[0])
    assert rows == [(1, "First"), (2, "Second"), (3, "Third with unicode पांचाल")]
