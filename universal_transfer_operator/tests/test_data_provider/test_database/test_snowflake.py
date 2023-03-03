"""Tests specific to the Snowflake Database implementation."""
import pathlib

import pandas as pd
import pytest
import sqlalchemy
import utils.test_utils as test_utils
from sqlalchemy.exc import ProgrammingError

from universal_transfer_operator.constants import FileType, TransferMode
from universal_transfer_operator.data_providers.database.snowflake import SnowflakeDataProvider
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.settings import SNOWFLAKE_SCHEMA

DEFAULT_CONN_ID = "snowflake_default"
CUSTOM_CONN_ID = "snowflake_conn"
SUPPORTED_CONN_IDS = [CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "cols_eval",
    [
        # {"cols": ["SELL", "LIST"], "expected_result": False},
        {"cols": ["Sell", "list"], "expected_result": True},
        {"cols": ["sell", "List"], "expected_result": True},
        {"cols": ["sell", "lIst"], "expected_result": True},
        {"cols": ["sEll", "list"], "expected_result": True},
        {"cols": ["sell", "LIST"], "expected_result": False},
        {"cols": ["sell", "list"], "expected_result": False},
    ],
)
def test_use_quotes(cols_eval):
    """
    Verify the quotes addition only in case where we are having mixed case col names
    """
    dp = SnowflakeDataProvider(dataset=Table(name="some_table"), transfer_mode=TransferMode.NONNATIVE)
    assert dp.use_quotes(cols_eval["cols"]) == cols_eval["expected_result"]


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

# Since the LocalDatasetPtovider is not yet added in this PR
# commenting this code once that it is added we can run this test case.
# @pytest.mark.integration
# @pytest.mark.parametrize(
#     "dataset_table_fixture",
#     [
#         {
#             "dataset": "SnowflakeDataProvider",
#             "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
#         },
#     ],
#     indirect=True,
#     ids=["snowflake"],
# )
# def test_load_file_to_table(dataset_table_fixture):
#     """Test loading on files to snowflake database"""
#     database, target_table = dataset_table_fixture
#     filepath = str(pathlib.Path(CWD.parent, "data/sub_folder/"))
#     database.load_file_to_table(File(filepath, filetype=FileType.CSV), target_table, {})

#     df = database.hook.get_pandas_df(f"SELECT * FROM {database.get_table_qualified_name(target_table)}")
#     assert len(df) == 3
#     expected = pd.DataFrame(
#         [
#             {"id": 1, "name": "First"},
#             {"id": 2, "name": "Second"},
#             {"id": 3, "name": "Third with unicode पांचाल"},
#         ]
#     )
#     test_utils.assert_dataframes_are_equal(df, expected)
