import pathlib
from unittest import mock

import pandas
import pytest

from astro import sql as aql

from ..operators import utils as test_utils

CWD = pathlib.Path(__file__).parent
DATA_FILEPATH = pathlib.Path(CWD.parent.parent, "data/sample.csv")


@pytest.mark.parametrize("rows", [([], []), (["a", "b"], ["a", "b"]), (1, 1), (None, None)])
def test_make_row_serializable(rows):
    """
    Test make_row_serializable() only modifies SQLAlcRow object
    """
    input_rows, expected_output = rows
    result = aql.RawSQLOperator.make_row_serializable(input_rows)
    assert result == expected_output


@mock.patch("astro.sql.operators.raw_sql.RawSQLOperator.results_as_list")
@mock.patch("astro.databases.base.BaseDatabase.connection")
def test_run_sql_calls_list_handler(run_sql, results_as_list, sample_dag):
    results_as_list.return_value = []
    run_sql.execute.return_value = []
    with sample_dag:

        @aql.run_raw_sql(results_format="list", conn_id="sqlite_default")
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()

    test_utils.run_dag(sample_dag)
    results_as_list.assert_called_with([])


@mock.patch("astro.sql.operators.raw_sql.RawSQLOperator.results_as_pandas_dataframe")
@mock.patch("astro.databases.base.BaseDatabase.connection")
def test_run_sql_calls_pandas_dataframe_handler(run_sql, results_as_pandas_dataframe, sample_dag):
    results_as_pandas_dataframe.return_value = []
    run_sql.execute.return_value = []
    with sample_dag:

        @aql.run_raw_sql(results_format="pandas_dataframe", conn_id="sqlite_default")
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()

    test_utils.run_dag(sample_dag)
    results_as_pandas_dataframe.assert_called_with([])


@mock.patch("astro.sql.operators.raw_sql.RawSQLOperator.results_as_pandas_dataframe")
@mock.patch("astro.databases.base.BaseDatabase.connection")
def test_run_sql_gives_priority_to_pandas_dataframe_handler(run_sql, results_as_pandas_dataframe, sample_dag):
    """
    Test that run_sql calls `results_format` specified handler over handler passed in decorator.
    """
    results_as_pandas_dataframe.return_value = []
    run_sql.execute.return_value = []
    with sample_dag:

        @aql.run_raw_sql(
            results_format="pandas_dataframe", conn_id="sqlite_default", handler=lambda x: x.fetchall()
        )
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()

    test_utils.run_dag(sample_dag)
    results_as_pandas_dataframe.assert_called_with([])


@mock.patch("astro.sql.operators.raw_sql.RawSQLOperator.results_as_pandas_dataframe")
@mock.patch("astro.databases.base.BaseDatabase.run_sql")
def test_run_sql_called_handler(run_sql, results_as_pandas_dataframe, sample_dag):
    """
    Test that run_sql calls `handler` passed in decorator.
    """
    results_as_pandas_dataframe.return_value = []
    return_value = [1, 2, 3]
    run_sql.return_value = return_value

    def verify(result):
        assert result == return_value
        return result

    with sample_dag:

        @aql.run_raw_sql(conn_id="sqlite_default", handler=verify)
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()
    test_utils.run_dag(sample_dag)


@mock.patch("astro.sql.operators.raw_sql.RawSQLOperator.results_as_pandas_dataframe")
def test_run_sql_should_raise_exception(results_as_pandas_dataframe, sample_dag):
    """
    Test that run_sql should raise an exception when fail_on_empty=False
    """
    results_as_pandas_dataframe.return_value = []

    def raise_exception(result):
        raise ValueError("dummy exception")

    with pytest.raises(ValueError, match="dummy exception"):
        with sample_dag:

            @aql.run_raw_sql(conn_id="sqlite_default", handler=raise_exception, fail_on_empty=True)
            def dummy_method():
                return "SELECT 1+1"

            dummy_method()
        test_utils.run_dag(sample_dag)


@mock.patch("astro.sql.operators.raw_sql.RawSQLOperator.results_as_pandas_dataframe")
@mock.patch("astro.databases.base.BaseDatabase.run_sql")
def test_run_sql_should_not_raise_exception(run_sql, results_as_pandas_dataframe, sample_dag):
    """
    Test that run_sql should not raise an exception when fail_on_empty=True
    """
    results_as_pandas_dataframe.return_value = []
    return_value = [1, 2, 3]
    run_sql.return_value = return_value

    def raise_exception(result):
        raise ValueError("dummy exception")

    with sample_dag:

        @aql.run_raw_sql(conn_id="sqlite_default", handler=raise_exception, fail_on_empty=False)
        def dummy_method():
            return "SELECT 1+1"

        dummy_method()
    test_utils.run_dag(sample_dag)


def test_handlers():
    """
    Test the handler return desired results
    """

    class Val:
        def __init__(self, val):
            self.value = [val]

        def values(self) -> list:
            return self.value

    class MockResultProxy:
        @staticmethod
        def fetchall():
            return [Val(1), Val(2), Val(3)]

        @staticmethod
        def keys():
            return ["col"]

    result = MockResultProxy()
    processed_result = aql.RawSQLOperator.results_as_list(result)
    assert processed_result == [[1], [2], [3]]

    processed_result = aql.RawSQLOperator.results_as_pandas_dataframe(result)
    assert isinstance(processed_result, pandas.DataFrame)
    assert processed_result.shape == (3, 1)
