import pathlib

import pandas as pd
import pytest

from astro import sql as aql
from astro.constants import Database
from astro.files import File
from astro.table import Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


@aql.dataframe
def validate_basic(df: pd.DataFrame):
    assert len(df) == 6
    assert not df["sell"].hasnans
    assert df["rooms"].hasnans


@aql.dataframe
def validate_append_all(df: pd.DataFrame):
    assert len(df) == 6
    assert not df["sell"].hasnans
    assert not df["rooms"].hasnans


@aql.dataframe
def validate_caste_only(df: pd.DataFrame):
    assert len(df) == 6
    assert not df["age"].hasnans
    assert df["sell"].hasnans


@pytest.fixture
def append_params(request):
    mode = request.param
    if mode == "basic":
        return {
            "columns": {"sell": "sell", "living": "living"},
        }, validate_basic
    if mode == "all_fields":
        return {}, validate_append_all


@pytest.mark.integration
@pytest.mark.parametrize(
    "append_params",
    ["basic", "all_fields"],
    indirect=True,
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.SQLITE},
        {"database": Database.REDSHIFT},
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift", "mssql"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {
                    "file": File(path=str(CWD) + "/../../data/homes_main.csv"),
                },
                {
                    "file": File(path=str(CWD) + "/../../data/homes_append.csv"),
                },
            ],
        }
    ],
    indirect=True,
)
def test_append(database_table_fixture, sample_dag, multiple_tables_fixture, append_params):
    app_param, validate_append = append_params
    main_table, append_table = multiple_tables_fixture
    with sample_dag:
        appended_table = aql.append(
            **app_param,
            target_table=main_table,
            source_table=append_table,
        )
        validate_append(appended_table)
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "append_params",
    ["all_fields"],
    indirect=True,
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.DELTA},
    ],
    indirect=True,
    ids=["delta"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {
                    "file": File(path=str(CWD) + "/../../data/homes_main.csv"),
                },
                {
                    "file": File(path=str(CWD) + "/../../data/homes_append.csv"),
                },
            ],
        }
    ],
    indirect=True,
)
def test_append_delta(database_table_fixture, sample_dag, multiple_tables_fixture, append_params):
    # TODO: We currently can not get the "basic" to work because Delta does not yet support default values.
    app_param, validate_append = append_params
    main_table, append_table = multiple_tables_fixture
    with sample_dag:
        appended_table = aql.append(
            **app_param,
            target_table=main_table,
            source_table=append_table,
        )
        validate_append(appended_table)
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.POSTGRES}],
    indirect=True,
    ids=["postgresql"],
)
def test_append_on_tables_on_different_db(sample_dag, database_table_fixture):
    test_table_1 = Table(conn_id="postgres_conn")
    test_table_2 = Table(conn_id="sqlite_conn")
    with pytest.raises(ValueError) as exec_info:
        with sample_dag:
            load_main = aql.load_file(
                input_file=File(path=str(CWD) + "/../../data/homes_main.csv"),
                output_table=test_table_1,
            )
            load_append = aql.load_file(
                input_file=File(path=str(CWD) + "/../../data/homes_append.csv"),
                output_table=test_table_2,
            )
            aql.append(
                target_table=load_main,
                source_table=load_append,
            )
        test_utils.run_dag(sample_dag)
    assert exec_info.value.args[0] == "source and target table must belong to the same datasource"
