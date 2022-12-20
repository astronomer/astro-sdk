from unittest import mock

import pytest

from astro.sql.operators.transform import TransformOperator
from astro.table import BaseTable, Table
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
