import pathlib

import pandas as pd
import pytest
from pandas import DataFrame

from astro.constants import SUPPORTED_DATABASES
from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table

CWD = pathlib.Path(__file__).parent


class DatabaseSubclass(BaseDatabase):
    pass


def test_subclass_missing_not_implemented_methods_raise_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
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


def test_subclass_missing_load_pandas_dataframe_to_table_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    table = Table()
    df = DataFrame()
    with pytest.raises(NotImplementedError):
        db.load_pandas_dataframe_to_table(df, table)


def test_subclass_missing_append_table_raises_exception():
    db = DatabaseSubclass(conn_id="fake_conn_id")
    source_table = Table()
    target_table = Table()
    with pytest.raises(NotImplementedError):
        db.combine_tables(source_table, target_table, {})


@pytest.mark.parametrize(
    "sql_server",
    SUPPORTED_DATABASES,
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        [
            {
                "param": {"metadata": Metadata(schema=SCHEMA)},
                "path": str(CWD) + "/../data/simple_merge_1.csv",
                "load_table": True,
            },
            {
                "param": {"metadata": Metadata(schema=SCHEMA)},
                "path": str(CWD) + "/../data/simple_merge_2.csv",
                "load_table": True,
            },
        ],
    ],
    indirect=True,
    ids=["table"],
)
@pytest.mark.parametrize(
    "parameters",
    [
        {
            "source_to_target_columns_map": {"sell": "sell", "list": "list"},
            "expected_df": pd.DataFrame(
                [
                    {"sell": 142, "list": 160},
                    {"sell": 162, "list": 330},
                    {"sell": 175, "list": 600},
                    {"sell": 175, "list": 540},
                ]
            ),
        }
    ],
)
def test_append_table(parameters, test_table, sql_server):
    sql_name, hook = sql_server
    lst, sell = get_col_names(sql_name)
    db = create_database(conn_id=test_table[0].conn_id)
    db.combine_tables(
        target_table=test_table[0],
        source_table=test_table[1],
        source_to_target_columns_map=parameters["source_to_target_columns_map"],
    )
    df = db.hook.get_pandas_df(
        f"SELECT * FROM {db.get_table_qualified_name(test_table[0])}"
    )
    df = df.sort_values(by=[lst], ascending=True)
    parameters["expected_df"] = parameters["expected_df"].sort_values(
        by=["list"], ascending=True
    )

    assert df[lst].to_list() == parameters["expected_df"]["list"].to_list()
    assert df[sell].to_list() == parameters["expected_df"]["sell"].to_list()


@pytest.mark.parametrize(
    "sql_server",
    SUPPORTED_DATABASES,
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        [
            {
                "param": {"metadata": Metadata(schema=SCHEMA)},
                "path": str(CWD) + "/../data/simple_merge_1.csv",
                "load_table": True,
            },
            {
                "param": {"metadata": Metadata(schema=SCHEMA)},
                "path": str(CWD) + "/../data/simple_merge_2.csv",
                "load_table": True,
            },
        ],
    ],
    indirect=True,
    ids=["table"],
)
@pytest.mark.parametrize(
    "parameters",
    [
        {
            "target_conflict_columns": ["sell"],
            "source_to_target_columns_map": {"sell": "sell", "list": "list"},
            "if_conflict": "update",
            "expected_df": pd.DataFrame(
                [
                    {"sell": 142, "list": 160},
                    {"sell": 162, "list": 330},
                    {"sell": 175, "list": 600},
                ]
            ),
        },
        {
            "target_conflict_columns": ["sell"],
            "source_to_target_columns_map": {"sell": "sell", "list": "list"},
            "if_conflict": "ignore",
            "expected_df": pd.DataFrame(
                [
                    {"sell": 142, "list": 160},
                    {"sell": 162, "list": 330},
                    {"sell": 175, "list": 540},
                ]
            ),
        },
    ],
)
def test_merge_table(parameters, test_table, sql_server):
    sql_name, hook = sql_server
    lst, sell = get_col_names(sql_name)
    target_conflict_columns = parameters["target_conflict_columns"]
    db = create_database(conn_id=test_table[0].conn_id)

    add_constraint(db, sql_name, test_table, target_conflict_columns)

    db.combine_tables(
        target_table=test_table[0],
        source_table=test_table[1],
        target_conflict_columns=target_conflict_columns,
        if_conflict=parameters["if_conflict"],
        source_to_target_columns_map=parameters["source_to_target_columns_map"],
    )
    df = db.hook.get_pandas_df(
        f"SELECT * FROM {db.get_table_qualified_name(test_table[0])}"
    )
    df = df.sort_values(by=[lst], ascending=True)

    parameters["expected_df"] = parameters["expected_df"].sort_values(
        by=["list"], ascending=True
    )

    assert df[lst].to_list() == parameters["expected_df"]["list"].to_list()
    assert df[sell].to_list() == parameters["expected_df"]["sell"].to_list()


def add_constraint(db, sql_name, test_table, target_conflict_columns):
    if sql_name == "postgres":
        db.run_sql(
            sql_statement=f"ALTER TABLE {db.get_table_qualified_name(test_table[0])} "
            f"ADD CONSTRAINT con_{test_table[0].name} UNIQUE ({','.join(target_conflict_columns)})"
        )
    if sql_name == "sqlite":
        db.run_sql(
            f"CREATE UNIQUE INDEX unique_index ON {test_table[0].name}"
            + f"({','.join(target_conflict_columns)})"
        )


def get_col_names(sql_name):
    lst = "list"
    sell = "sell"
    if sql_name == "snowflake":
        lst = lst.upper()
        sell = sell.upper()
    return lst, sell
