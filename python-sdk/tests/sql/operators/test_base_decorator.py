from astro.sql.operators.base_decorator import BaseSQLDecoratedOperator


def test_base_sql_decorated_operator_template_fields_with_parameters():
    """
    Test that parameters is in BaseSQLDecoratedOperator template_fields
     as this required for taskflow to work if XCom args are being passed via parameters.
    """
    assert "parameters" in BaseSQLDecoratedOperator.template_fields
