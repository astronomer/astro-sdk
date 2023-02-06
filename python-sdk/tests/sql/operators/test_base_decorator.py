from unittest import mock

import pandas as pd
import pytest

from astro.sql import RawSQLOperator
from astro.sql.operators.base_decorator import (
    BaseSQLDecoratedOperator,
    load_op_arg_dataframes_into_sql,
    load_op_kwarg_dataframes_into_sql,
)
from astro.table import BaseTable, Table


def test_base_sql_decorated_operator_template_fields_with_parameters():
    """
    Test that parameters is in BaseSQLDecoratedOperator template_fields
     as this required for taskflow to work if XCom args are being passed via parameters.
    """
    assert "parameters" in BaseSQLDecoratedOperator.template_fields


@pytest.mark.parametrize("exception", [OSError("os error"), TypeError("type error")])
@mock.patch("astro.sql.operators.base_decorator.inspect.getsource", autospec=True)
def test_get_source_code_handle_exception(mock_getsource, exception):
    """assert get_source_code not raise exception"""
    mock_getsource.side_effect = exception
    RawSQLOperator(task_id="test", sql="select * from 1", python_callable=lambda: 1).get_source_code(
        py_callable=None
    )


def test_load_op_arg_dataframes_into_sql():
    df_1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df_2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    op_args = (df_1, df_2, Table(conn_id="sqlite_default"), "str")
    results = load_op_arg_dataframes_into_sql(
        conn_id="sqlite_default", op_args=op_args, output_table=Table(conn_id="sqlite_default")
    )

    assert isinstance(results[0], BaseTable)
    assert isinstance(results[1], BaseTable)
    assert results[0].name != results[1].name

    assert isinstance(results[2], BaseTable)
    assert isinstance(results[3], str)


def test_load_op_kwarg_dataframes_into_sql():
    df_1 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    df_2 = pd.DataFrame({"col1": [1, 2], "col2": [3, 4]})
    op_kwargs = {"df_1": df_1, "df_2": df_2, "table": Table(conn_id="sqlite_default"), "some_str": "str"}
    results = load_op_kwarg_dataframes_into_sql(
        conn_id="sqlite_default", op_kwargs=op_kwargs, output_table=Table(conn_id="sqlite_default")
    )

    assert isinstance(results["df_1"], BaseTable)
    assert isinstance(results["df_2"], BaseTable)
    assert results["df_1"].name != results["df_2"].name

    assert isinstance(results["table"], BaseTable)
    assert isinstance(results["some_str"], str)


def test_base_sql_decorated_operator_template_fields_and_template_ext_with_sql():
    """
    Test that sql is in BaseSQLDecoratedOperator template_fields and template_ext
     as this required for rending the sql in the task instance rendered section.
    """
    assert "sql" in BaseSQLDecoratedOperator.template_fields
    assert ".sql" in BaseSQLDecoratedOperator.template_ext
