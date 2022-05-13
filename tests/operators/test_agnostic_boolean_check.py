"""
Unittest module to test Operators.
Requires the unittest, pytest, and requests-mock Python libraries.
"""

import logging
import os
import pathlib

import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES
from astro.settings import SCHEMA
from astro.sql.operators.agnostic_boolean_check import Check
from astro.sql.tables import Metadata, Table
from tests.operators.utils import get_table_name, run_dag

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


def drop_table_snowflake(
    table_name: str,
    conn_id: str = "snowflake_conn",
    schema: str = os.environ["SNOWFLAKE_SCHEMA"],
    database: str = os.environ["SNOWFLAKE_DATABASE"],
    warehouse: str = os.environ["SNOWFLAKE_WAREHOUSE"],
):
    hook = SnowflakeHook(
        snowflake_conn_id=conn_id,
        schema=schema,
        database=database,
        warehouse=warehouse,
    )
    snowflake_conn = hook.get_conn()
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


@pytest.fixture(scope="module")
def table(request):
    boolean_check_table = Table(
        name=get_table_name("boolean_check_test"),
        metadata=Metadata(
            database="pagila",
            schema="airflow_test_dag",
        ),
        conn_id="postgres_conn",
    )
    boolean_check_table_bigquery = Table(
        name=get_table_name("boolean_check_test"),
        conn_id="bigquery",
        metadata=Metadata(schema=SCHEMA),
    )
    boolean_check_table_sqlite = Table(
        name=get_table_name("boolean_check_test"), conn_id="sqlite_conn"
    )
    boolean_check_table_snowflake = Table(
        name=get_table_name("boolean_check_test"),
        metadata=Metadata(
            database=os.getenv("SNOWFLAKE_DATABASE"),  # type: ignore
            schema=os.getenv("SNOWFLAKE_SCHEMA"),  # type: ignore
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # type: ignore
        ),
        conn_id="snowflake_conn",
    )
    path = str(CWD) + "/../data/homes_append.csv"
    tables = {
        "postgres": boolean_check_table,
        "bigquery": boolean_check_table_bigquery,
        "sqlite": boolean_check_table_sqlite,
        "snowflake": boolean_check_table_snowflake,
    }

    aql.load_file(
        path=path,
        output_table=tables[request.param],
    ).operator.execute({"run_id": "foo"})

    yield tables[request.param]

    tables[request.param].drop()


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_happyflow_success(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        temp_table = get_table(table)
        aql.boolean_check(
            table=temp_table,
            checks=[Check("test_1", "rooms > 3")],
            max_rows_returned=10,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("table", SUPPORTED_DATABASES, indirect=True)
def test_happyflow_fail(sample_dag, table, caplog):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            temp_table = get_table(table)
            aql.boolean_check(
                table=temp_table,
                checks=[
                    Check("test_1", "rooms > 7"),
                    Check("test_2", "beds >= 3"),
                ],
                max_rows_returned=10,
            )
        run_dag(sample_dag)
    expected_error = "Some of the check(s) have failed"
    assert expected_error in caplog.text


@pytest.mark.parametrize(
    "table",
    [
        "postgres",
        pytest.param(
            "bigquery",
            marks=pytest.mark.xfail(
                reason="bigquery don't expect table name before cols."
            ),
        ),
        pytest.param(
            "snowflake",
            marks=pytest.mark.xfail(
                reason="Binding data in type (table) is not supported."
            ),
        ),
        "sqlite",
    ],
    indirect=True,
)
def test_happyflow_success_with_templated_query(sample_dag, table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        temp_table = get_table(table)
        aql.boolean_check(
            table=temp_table,
            checks=[Check("test_1", "{{table}}.rooms > 3")],
            max_rows_returned=10,
        )
    run_dag(sample_dag)
