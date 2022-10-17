from unittest import mock

import pytest

from astro.files import File
from astro.sql import LoadFileOperator
from astro.sql.operators.transform import TransformOperator
from astro.table import BaseTable, Table, Metadata
from astro.utils.table import find_first_table


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
def test_row_count():
    imdb_table = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema="astro")),
    ).execute({})

    assert imdb_table.row_count > 0
