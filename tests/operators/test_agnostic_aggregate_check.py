import logging
import os
import pathlib

import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.utils import timezone

import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES, Database
from astro.settings import SCHEMA
from astro.sql.tables import Table
from tests.operators.utils import get_table_name, run_dag

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


@pytest.fixture(scope="module")
def table(request):
    aggregate_table = Table(
        "aggregate_check_test",
        database="pagila",
        conn_id="postgres_conn",
        schema="airflow_test_dag",
    )
    aggregate_table_bigquery = Table(
        "aggregate_check_test",
        conn_id="bigquery",
        schema=SCHEMA,
    )
    aggregate_table_sqlite = Table("aggregate_check_test", conn_id="sqlite_conn")
    aggregate_table_snowflake = Table(
        table_name=get_table_name("aggregate_check_test"),
        database=os.getenv("SNOWFLAKE_DATABASE"),  # type: ignore
        schema=os.getenv("SNOWFLAKE_SCHEMA"),  # type: ignore
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # type: ignore
        conn_id="snowflake_conn",
    )
    path = str(CWD) + "/../data/homes_merge_1.csv"
    tables = {
        "postgres": aggregate_table,
        "bigquery": aggregate_table_bigquery,
        "sqlite": aggregate_table_sqlite,
        "snowflake": aggregate_table_snowflake,
    }
    aql.load_file(
        path=path,
        output_table=tables[request.param],
    ).operator.execute({"run_id": "foo"})

    yield tables[request.param]

    tables[request.param].drop()


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_range_values(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(table)
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            greater_than=4,
            less_than=4,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_out_of_range_value(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            aggregate_table = get_table(table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=10,
                less_than=20,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_equal_to_param(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(table)
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            equal_to=4,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_only_less_than_param(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            aggregate_table = get_table(table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                less_than=3,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_only_greater_than_param(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(table)
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            greater_than=3,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_all_three_params_provided_priority_given_to_equal_to_param(sample_dag, table):
    """greater_than should be less than or equal to less_than"""

    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(ValueError):
        with sample_dag:
            aggregate_table = get_table(table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=20,
                less_than=10,
                equal_to=4,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("table", [Database.SQLITE])
def test_invalid_params_no_test_values(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(ValueError):
        with sample_dag:
            aggregate_table = get_table(table)
            aql.aggregate_check(
                table=aggregate_table, check="select count(*) FROM {{table}}"
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("table", [Database.SQLITE])
def test_invalid_values(sample_dag, table):
    """greater_than should be less than or equal to less_than"""

    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(ValueError):
        with sample_dag:
            aggregate_table = get_table(table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=20,
                less_than=10,
            )
        run_dag(sample_dag)
