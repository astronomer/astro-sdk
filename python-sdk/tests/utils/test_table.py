from unittest import mock

import pytest

from astro.constants import Database
from astro.files import File
from astro.sql import LoadFileOperator
from astro.sql.operators.transform import TransformOperator
from astro.table import BaseTable, Table
from astro.utils.table import find_first_table
from tests.utils.airflow import create_context


@pytest.mark.parametrize(
    "kwargs,return_type",
    [
        (
            {
                "op_args": (),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {},
            },
            type(None),
        ),
        (
            {
                "op_args": (Table(),),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {"table": Table()},
                "python_callable": lambda table: table,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {"table": Table()},
            },
            BaseTable,
        ),
    ],
    ids=["none", "op_args", "op_kwargs", "parameters"],
)
def test_find_first_table(kwargs, return_type):
    assert isinstance(find_first_table(context={}, **kwargs), return_type)


@pytest.mark.parametrize(
    "kwargs,return_type",
    [
        (
            {
                "op_args": (TransformOperator(python_callable=lambda: "select 1").output,),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {"table": TransformOperator(python_callable=lambda: "select 1").output},
                "python_callable": lambda table: table,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {"table": TransformOperator(python_callable=lambda: "select 1").output},
            },
            BaseTable,
        ),
    ],
    ids=["op_args", "op_kwargs", "parameters"],
)
@mock.patch("airflow.models.xcom_arg.PlainXComArg.resolve", return_value=Table())
def test_find_first_table_with_xcom_arg(xcom_arg_resolve, kwargs, return_type):
    assert isinstance(find_first_table(context={}, **kwargs), return_type)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.SQLITE,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_row_count(database_table_fixture):
    """
    Load file in bigquery and test the row count of bigquery table
    """
    _, test_table = database_table_fixture
    load_file = LoadFileOperator(
        task_id="load_file",
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
        ),
        output_table=test_table,
    )
    imdb_table = load_file.execute(context=create_context(load_file))
    assert imdb_table.row_count == 117


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.REDSHIFT},
        {"database": Database.SNOWFLAKE},
        {"database": Database.SQLITE},
    ],
    indirect=True,
    ids=["bigquery", "postgresql", "redshift", "snowflake", "sqlite"],
)
def test_sql_type(database_table_fixture, request):
    _, test_table = database_table_fixture
    assert test_table.sql_type == request.node.callspec.id
