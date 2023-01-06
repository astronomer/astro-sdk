from unittest import mock
import pytest

from astro.sql import RawSQLOperator
from astro.sql.operators.base_decorator import BaseSQLDecoratedOperator


def test_base_sql_decorated_operator_template_fields_with_parameters():
    """
    Test that parameters is in BaseSQLDecoratedOperator template_fields
     as this required for taskflow to work if XCom args are being passed via parameters.
    """
    assert "parameters" in BaseSQLDecoratedOperator.template_fields


@pytest.mark.parametrize(
    "exception", [OSError("os error"), TypeError("type error")]
)
@mock.patch("astro.sql.operators.base_decorator.inspect.getsource", autospec=True)
def test_get_source_code_handle_exception(mock_getsource, exception):
    """assert get_source_code not raise exception"""
    mock_getsource.side_effect = exception
    RawSQLOperator(
        task_id="test",
        sql="select * from 1",
        python_callable=lambda: 1
    ).get_source_code(py_callable=None)
