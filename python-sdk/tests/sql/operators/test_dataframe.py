import os
import pathlib
from unittest import mock

import pandas
import pytest
from airflow.exceptions import AirflowException
from airflow.models.xcom import BaseXCom
from airflow.utils import timezone

import astro.sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import Database
from astro.custom_backend.astro_custom_backend import AstroCustomXcomBackend as XCom
from astro.files import File
from astro.table import Table
from tests.sql.operators import utils as test_utils

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_dataframe_from_sql_basic(sample_dag, database_table_fixture):
    """Test basic operation of dataframe operator."""

    _, test_table = database_table_fixture

    @aql.dataframe
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        f = my_df_func(df=test_table)

    test_utils.run_dag(sample_dag)

    assert XCom.get_one(execution_date=DEFAULT_DATE, key=f.key, task_id=f.operator.task_id) == 5


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_dataframe_from_sql_custom_task_id(sample_dag, database_table_fixture):
    """Test custom and taskId increment when same task is added multiple times."""

    _, test_table = database_table_fixture

    @aql.dataframe(task_id="foo")
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        for _ in range(5):
            # ensure we can create multiple tasks
            my_df_func(df=test_table)

    task_ids = [x.task_id for x in sample_dag.tasks]
    assert task_ids == ["foo", "foo__1", "foo__2", "foo__3", "foo__4"]


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_dataframe_from_sql_basic_op_arg(sample_dag, database_table_fixture):
    """Test basic operation of dataframe operator with op_args."""

    _, test_table = database_table_fixture

    @aql.dataframe(
        conn_id=test_table.conn_id,
        database=getattr(test_table.metadata, "database", None),
    )
    def my_df_func(df: pandas.DataFrame):  # skipcq: PY-D0003
        return df.sell.count()

    with sample_dag:
        res = my_df_func(test_table)
    test_utils.run_dag(sample_dag)

    assert XCom.get_one(execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id) == 5


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../data/homes2.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_dataframe_from_sql_basic_op_arg_and_kwarg(
    sample_dag,
    database_table_fixture,
):
    """Test dataframe creation from table object in args and kwargs."""
    test_table = database_table_fixture[1]

    @aql.dataframe(
        conn_id=test_table.conn_id,
        database=getattr(test_table.metadata, "database", None),
    )
    def my_df_func(df_1: pandas.DataFrame, df_2: pandas.DataFrame):  # skipcq: PY-D0003
        return df_1.sell.count() + df_2.sell.count()

    with sample_dag:
        res = my_df_func(test_table, df_2=test_table)
    test_utils.run_dag(sample_dag)

    assert XCom.get_one(execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id) == 10


def test_postgres_dataframe_without_table_arg(sample_dag):
    """Test dataframe operator without table argument"""

    @aql.dataframe
    def validate_result(df: pandas.DataFrame):  # skipcq: PY-D0003
        assert df.iloc[0].to_dict()["colors"] == "red"

    @aql.dataframe
    def sample_df():  # skipcq: PY-D0003
        return pandas.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})

    @aql.transform
    def sample_pg(input_table: Table):  # skipcq: PY-D0003
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        plain_df = sample_df()
        pg_df = sample_pg(conn_id="postgres_conn", database="pagila", input_table=plain_df)
        validate_result(pg_df)
    test_utils.run_dag(sample_dag)


test_df = pandas.DataFrame({"numbers": [1, 2, 3], "Colors": ["red", "white", "blue"]})
test_df_2 = pandas.DataFrame({"Numbers": [1, 2, 3], "Colors": ["red", "white", "blue"]})


def _validate_dataframe(original: pandas.DataFrame, df: pandas.DataFrame, capital_settings: dict):
    cols = list(df)
    assert len(df) == len(original)
    assert all(getattr(x, capital_settings["function"]) for x in cols)


def _validate_list(original: list, function_output: list, capital_settings: dict):
    assert len(original) == len(function_output)
    for pre, post in zip(function_output, original):
        assert isinstance(pre, pandas.DataFrame) == isinstance(post, pandas.DataFrame)
        if isinstance(pre, pandas.DataFrame):
            _validate_dataframe(pre, post, capital_settings)


def _validate_dict(x: dict, function_output: dict, capital_settings: dict):
    assert x.keys() == function_output.keys()
    for key in function_output.keys():
        post = x[key]
        pre = function_output[key]
        assert isinstance(pre, pandas.DataFrame) == isinstance(post, pandas.DataFrame)
        if isinstance(pre, pandas.DataFrame):
            _validate_dataframe(pre, post, capital_settings)


def _find_validator(function_output):
    if isinstance(function_output, list):
        return _validate_list
    elif isinstance(function_output, dict):
        return _validate_dict
    else:
        return _validate_dataframe


@pytest.mark.parametrize(
    "capital_settings",
    [
        {"column_setting": "upper", "function": "isupper"},
        {"column_setting": "lower", "function": "islower"},
        {"column_setting": "original", "function": "__eq__"},
    ],
    ids=["upper", "lower", "original"],
)
@pytest.mark.parametrize(
    "function_output",
    [
        [1, 2, test_df],
        [test_df, test_df_2],
        [test_df],
        {"foo": 1, "bar": 2, "baz": test_df},
        {"foo": test_df, "bar": test_df_2},
        {"foo": test_df},
        test_df,
        test_df_2,
    ],
    ids=[
        "mixed_list",
        "two_df_list",
        "single_df_list",
        "mixed_dict",
        "two_df_dict",
        "single_df_dict",
        "single_df",
        "single_df_mixed",
    ],
)
def test_columns_name_cap_multi_output(sample_dag, capital_settings, function_output):

    validator = _find_validator(function_output)

    @aql.dataframe(columns_names_capitalization=capital_settings["column_setting"])
    def make_df():
        return function_output

    @aql.dataframe()
    def validate(x):
        validator(x, function_output, capital_settings)

    with sample_dag:
        validate(make_df())
    test_utils.run_dag(sample_dag)


def test_pass_table_multi_df(sample_dag):
    @aql.dataframe()
    def make_df():
        return [test_df, test_df_2]

    with pytest.raises(
        ValueError,
        match="Astro can only turn a single dataframe into a table. Please change your function output.",
    ):
        with sample_dag:
            make_df(output_table=Table())
        test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "kwargs",
    [{"task_id": "task1", "queue": "new_1"}, {"queue": "new_2", "owner": "astro-sdk"}],
)
def test_pass_kwargs_to_base_operator(kwargs):
    """Test that kwargs passed to decorator are passed to BaseOperator"""

    @aql.dataframe(**kwargs)
    def sample_df_1():  # skipcq: PY-D0003
        return pandas.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})

    task1 = sample_df_1()
    assert all(getattr(task1.operator, k) == v for k, v in kwargs.items())


@pytest.mark.skipif(not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    output_table = Table("test_name")

    @aql.dataframe()
    def sample_df_1(**kwargs):
        return pandas.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})

    task = sample_df_1(output_table=output_table)
    assert task.operator.outlets == [output_table]


@pytest.mark.skipif(DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    output_table = Table("test_name")

    @aql.dataframe()
    def sample_df_1(**kwargs):
        return pandas.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})

    task = sample_df_1(output_table=output_table)
    assert task.operator.outlets == []


def test_dataframe_from_file(sample_dag):
    @aql.dataframe
    def validate_file(df: pandas.DataFrame):  # skipcq: PY-D0003
        assert len(df) == 5
        assert "sell" in df.columns

    with sample_dag:
        validate_file(df=File(path=str(CWD) + "/../../data/homes2.csv"))
        validate_file(File(path=str(CWD) + "/../../data/homes2.csv"))
    test_utils.run_dag(sample_dag)


@mock.patch("airflow.models.xcom.XCom", BaseXCom)
@mock.patch("astro.custom_backend.serializer")
@mock.patch.dict(os.environ, {"AIRFLOW__CORE__ENABLE_XCOM_PICKLING": "True"})
def test_dataframe_from_file_xcom_pickling(mock_serde, sample_dag):
    @aql.dataframe
    def validate_file(df: pandas.DataFrame):  # skipcq: PY-D0003
        assert len(df) == 5
        assert "sell" in df.columns
        return df

    @aql.dataframe()
    def count_df(df: pandas.DataFrame):
        return len(df)

    with sample_dag:
        count_df(validate_file(df=File(path=str(CWD) + "/../../data/homes2.csv")))
    test_utils.run_dag(sample_dag)
    mock_serde.serialize.assert_not_called()
    mock_serde.deserialize.assert_not_called()


@mock.patch("astro.settings.STORE_DATA_LOCAL_DEV", False)
def test_dataframe_no_storage_option_raises_exception(sample_dag):
    @aql.dataframe
    def validate_file(df: pandas.DataFrame):  # skipcq: PY-D0003
        assert len(df) == 5
        assert "sell" in df.columns
        return df

    @aql.dataframe()
    def count_df(df: pandas.DataFrame):
        return len(df)

    with pytest.raises(AirflowException, match="Since you have not provided a remote object storage conn_id"):
        with sample_dag:
            count_df(validate_file(df=File(path=str(CWD) + "/../../data/homes2.csv")))
        test_utils.run_dag(sample_dag)


def test_col_case_is_preserved(sample_dag):
    """Test that column case is preserved"""

    @aql.dataframe()
    def sample_df_1():  # skipcq: PY-D0003
        return pandas.DataFrame({"Numbers": [1, 2, 3], "Colors": ["red", "white", "blue"]})

    @aql.dataframe()
    def validate(df):  # skipcq: PY-D0003
        cols = list(df.columns)
        cols.sort()
        return df.columns == ["Colors", "Numbers"]

    with sample_dag:
        task1 = sample_df_1()
        validate(task1)
    test_utils.run_dag(sample_dag)
