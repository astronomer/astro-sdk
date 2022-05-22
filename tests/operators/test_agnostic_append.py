import pathlib

import pandas as pd
import pytest
from airflow.exceptions import BackfillUnfinished

from astro import sql as aql
from astro.dataframe import dataframe as adf
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table
from tests.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


@adf
def validate_basic(df: pd.DataFrame):
    assert len(df) == 6
    assert not df["sell"].hasnans
    assert df["rooms"].hasnans


@adf
def validate_append_all(df: pd.DataFrame):
    assert len(df) == 6
    assert not df["sell"].hasnans
    assert not df["rooms"].hasnans


@adf
def validate_caste_only(df: pd.DataFrame):
    assert len(df) == 6
    assert not df["age"].hasnans
    assert df["sell"].hasnans


@pytest.fixture
def append_params(request):
    mode = request.param
    if mode == "basic":
        return {
            "columns": ["sell", "living"],
        }, validate_basic
    if mode == "all_fields":
        return {}, validate_append_all
    if mode == "with_caste":
        return {
            "columns": ["sell", "living"],
            "casted_columns": {"age": "INTEGER"},
        }, validate_basic
    if mode == "caste_only":
        return {"casted_columns": {"age": "INTEGER"}}, validate_caste_only


@pytest.mark.parametrize(
    "append_params",
    ["basic", "all_fields", "with_caste"],
    indirect=True,
)
@pytest.mark.parametrize(
    "sql_server",
    [
        "bigquery",
        "snowflake",
        "postgres",
        "sqlite",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        [
            {
                "is_temp": False,
                "param": {
                    "metadata": Metadata(schema=SCHEMA),
                },
                "path": str(CWD) + "/../data/homes_main.csv",
                "load_table": True,
            },
            {
                "is_temp": False,
                "param": {"metadata": Metadata(schema=SCHEMA)},
                "path": str(CWD) + "/../data/homes_append.csv",
                "load_table": True,
            },
        ],
    ],
    indirect=True,
    ids=["table"],
)
def test_append(sql_server, sample_dag, test_table, append_params):
    app_param, validate_append = append_params

    with sample_dag:
        appended_table = aql.append(
            **app_param,
            main_table=test_table[0],
            append_table=test_table[1],
        )
        validate_append(appended_table)
    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "sql_server",
    [
        "postgres",
    ],
    indirect=True,
)
def test_append_on_tables_on_different_db(sample_dag, sql_server):
    test_table_1 = Table(conn_id="postgres_conn")
    test_table_2 = Table(conn_id="sqlite_conn")
    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            load_main = aql.load_file(
                input_file=File(path=str(CWD) + "/../data/homes_main.csv"),
                output_table=test_table_1,
            )
            load_append = aql.load_file(
                input_file=File(path=str(CWD) + "/../data/homes_append.csv"),
                output_table=test_table_2,
            )
            aql.append(
                main_table=load_main,
                append_table=load_append,
            )
        test_utils.run_dag(sample_dag)
