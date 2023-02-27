import pathlib
from unittest import mock
from unittest.mock import call

import airflow
import pandas
import pytest

import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES, Database
from astro.databases import create_database
from astro.files import File
from astro.sql.operators.load_file import LoadFileOperator
from astro.table import Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent

DEFAULT_FILEPATH = str(pathlib.Path(CWD.parent.parent, "data/sample.csv").absolute())
SUPPORTED_DATABASES_OBJECTS = [
    {
        "database": database,
    }
    for database in Database
]
SUPPORTED_DATABASES_OBJECTS_WITH_FILE = [
    {
        "database": database,
        "file": File(DEFAULT_FILEPATH),
    }
    for database in Database
]

drop_table_statement = "DROP TABLE IF EXISTS {table_name}"


@pytest.mark.parametrize(
    "temp_table",
    [
        Table(conn_id="sqlite_conn"),
        Table(conn_id="snowflake_conn"),
        Table(conn_id="bigquery"),
        Table(conn_id="databricks_conn"),
        # Table(conn_id="redshift_conn"),
        Table(conn_id="postgres_conn"),
        Table(conn_id="duckdb_conn"),
    ],
    ids=["sqlite", "snowflake", "bigquery", "databricks", "postgres", "duckdb"],
)
def test_cleanup_one_table(temp_table):
    module = create_database(temp_table.conn_id)
    with mock.patch(f"{module.__class__.__module__}.{module.__class__.__name__}.run_sql") as mock_run_sql:
        a = aql.cleanup([temp_table])
        a.execute({})
    mock_run_sql.assert_called_once_with(
        drop_table_statement.format("table_name", table_name=temp_table.name)
    )


@pytest.mark.parametrize(
    "temp_table, non_temp_table",
    [
        (Table(conn_id="sqlite_conn"), Table(name="foo", conn_id="sqlite_conn")),
        (Table(conn_id="snowflake_conn"), Table(name="foo", conn_id="snowflake_conn")),
        (Table(conn_id="bigquery"), Table(name="foo", conn_id="bigquery")),
        (Table(conn_id="databricks_conn"), Table(name="foo", conn_id="databricks_conn")),
        # (Table(conn_id="redshift_conn"), Table(name="foo", conn_id="redshift_conn")),
        (Table(conn_id="postgres_conn"), Table(name="foo", conn_id="postgres_conn")),
        (Table(conn_id="duckdb_conn"), Table(name="foo", conn_id="duckdb_conn")),
    ],
    ids=["sqlite", "snowflake", "bigquery", "databricks", "postgres", "duckdb"],
)
def test_cleanup_non_temp_table(temp_table, non_temp_table):
    module = create_database(temp_table.conn_id)
    with mock.patch(f"{module.__class__.__module__}.{module.__class__.__name__}.run_sql") as mock_run_sql:
        a = aql.cleanup([temp_table, non_temp_table])
        a.execute({})
    mock_run_sql.assert_called_once_with(
        drop_table_statement.format("table_name", table_name=temp_table.name)
    )


@pytest.mark.parametrize(
    "temp_table",
    [
        Table(conn_id="sqlite_conn"),
        Table(conn_id="snowflake_conn"),
        Table(conn_id="bigquery"),
        Table(conn_id="databricks_conn"),
        # Table(conn_id="redshift_conn"),
        Table(conn_id="postgres_conn"),
        Table(conn_id="duckdb_conn"),
    ],
    ids=["sqlite", "snowflake", "bigquery", "databricks", "postgres", "duckdb"],
)
def test_cleanup_non_table(temp_table):
    df = pandas.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    module = create_database(temp_table.conn_id)
    with mock.patch(f"{module.__class__.__module__}.{module.__class__.__name__}.run_sql") as mock_run_sql:
        a = aql.cleanup([temp_table, df])
        a.execute({})
    mock_run_sql.assert_called_once_with(
        drop_table_statement.format("table_name", table_name=temp_table.name)
    )


@pytest.mark.parametrize(
    "temp_table_1, temp_table_2",
    [
        (Table(conn_id="sqlite_conn"), Table(conn_id="sqlite_conn")),
        (Table(conn_id="snowflake_conn"), Table(conn_id="snowflake_conn")),
        (Table(conn_id="bigquery"), Table(conn_id="bigquery")),
        (Table(conn_id="databricks_conn"), Table(conn_id="databricks_conn")),
        # (Table(conn_id="redshift_conn"), Table(conn_id="redshift_conn")),
        (Table(conn_id="postgres_conn"), Table(conn_id="postgres_conn")),
        (Table(conn_id="duckdb_conn"), Table(conn_id="duckdb_conn")),
    ],
    ids=["sqlite", "snowflake", "bigquery", "databricks", "postgres", "duckdb"],
)
def test_cleanup_multiple_table(temp_table_1, temp_table_2):
    df = pandas.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    module = create_database(temp_table_1.conn_id)
    with mock.patch(f"{module.__class__.__module__}.{module.__class__.__name__}.run_sql") as mock_run_sql:
        a = aql.cleanup([temp_table_1, temp_table_2, df])
        a.execute({})
    calls = [
        call(drop_table_statement.format("table_name", table_name=temp_table_1.name)),
        call(drop_table_statement.format("table_name", table_name=temp_table_2.name)),
    ]
    mock_run_sql.assert_has_calls(calls)


@pytest.mark.parametrize(
    "temp_table_1, temp_table_2",
    [
        (Table(conn_id="sqlite_conn"), Table(conn_id="sqlite_conn")),
        (Table(conn_id="bigquery"), Table(conn_id="bigquery")),
        (Table(conn_id="snowflake_conn"), Table(conn_id="snowflake_conn")),
        (Table(conn_id="databricks_conn"), Table(conn_id="databricks_conn")),
        # (Table(conn_id="redshift_conn"), Table(conn_id="redshift_conn")),
        (Table(conn_id="postgres_conn"), Table(conn_id="postgres_conn")),
        (Table(conn_id="duckdb_conn"), Table(conn_id="duckdb_conn")),
    ],
    ids=["sqlite", "bigquery", "snowflake", "databricks", "postgres", "duckdb"],
)
def test_cleanup_default_all_tables(temp_table_1, temp_table_2, sample_dag):
    @aql.transform()
    def foo(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        foo(temp_table_1, output_table=temp_table_2)

        aql.cleanup()
    module = create_database(temp_table_1.conn_id)
    with mock.patch(
        f"{module.__class__.__module__}.{module.__class__.__name__}.run_sql"
    ) as mock_run_sql, mock.patch(f"{module.__class__.__module__}.{module.__class__.__name__}.schema_exists"):
        test_utils.run_dag(sample_dag)
        db = create_database(temp_table_1.conn_id)
        calls = [
            call(
                drop_table_statement.format(
                    "table_name", table_name=db.get_table_qualified_name(temp_table_2)
                )
            ),
        ]
    mock_run_sql.assert_has_calls(calls)


@pytest.mark.integration
@pytest.mark.skipif(airflow.__version__ < "2.3.0", reason="Require Airflow version >= 2.3.0")
@pytest.mark.parametrize(
    "database_temp_table_fixture",
    SUPPORTED_DATABASES_OBJECTS,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
def test_cleanup_mapped_task(sample_dag, database_temp_table_fixture):
    db, temp_table = database_temp_table_fixture

    with sample_dag:
        load_file_mapped = LoadFileOperator.partial(task_id="load_file_mapped").expand_kwargs(
            [
                {
                    "input_file": File(path=(CWD.parent.parent / "data/sample.csv").as_posix()),
                    "output_table": temp_table,
                }
            ]
        )

        aql.cleanup(upstream_tasks=[load_file_mapped])
    test_utils.run_dag(sample_dag)

    assert not db.table_exists(temp_table)


@pytest.mark.integration
@pytest.mark.skipif(airflow.__version__ < "2.3.0", reason="Require Airflow version >= 2.3.0")
@pytest.mark.parametrize(
    "database_temp_table_fixture",
    SUPPORTED_DATABASES_OBJECTS,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
def test_cleanup_default_all_tables_mapped_task(sample_dag, database_temp_table_fixture):
    db, temp_table = database_temp_table_fixture

    with sample_dag:
        LoadFileOperator.partial(task_id="load_file_mapped").expand_kwargs(
            [
                {
                    "input_file": File(path=(CWD.parent.parent / "data/sample.csv").as_posix()),
                    "output_table": temp_table,
                }
            ]
        )

        aql.cleanup()
    test_utils.run_dag(sample_dag)

    assert not db.table_exists(temp_table)
