import pathlib

import pandas
import pytest
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.state import State

import astro.sql as aql
from astro.constants import Database
from astro.files import File
from astro.sql.operators.cleanup import CleanupOperator
from astro.sql.table import Table

CWD = pathlib.Path(__file__).parent

DEFAULT_FILEPATH = str(pathlib.Path(CWD.parent.parent, "data/sample.csv").absolute())
SQLITE_ONLY = [
    {
        "database": Database.SQLITE,
    },
]
SUPPORTED_DATABASES = [
    {
        "database": Database.SQLITE,
    },
    {
        "database": Database.POSTGRES,
    },
    {
        "database": Database.BIGQUERY,
    },
    {
        "database": Database.SNOWFLAKE,
    },
]
SUPPORTED_DATABASES_WITH_FILE = [
    dict(x, **{"file": File(DEFAULT_FILEPATH)}) for x in SUPPORTED_DATABASES
]


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_WITH_FILE,
    indirect=True,
    ids=["sqlite", "postgres", "bigquery", "snowflake"],
)
def test_cleanup_one_table(database_table_fixture):
    db, test_table = database_table_fixture
    assert db.table_exists(test_table)
    a = aql.cleanup([test_table])
    a.execute({})
    assert not db.table_exists(test_table)


@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES,
    indirect=True,
    ids=["sqlite", "postgres", "bigquery", "snowflake"],
)
@pytest.mark.parametrize(
    "tables_fixture",
    [
        {
            "items": [
                {
                    "table": Table(name="non_temp_table"),
                    "file": File(DEFAULT_FILEPATH),
                },
                {
                    "table": Table(),
                    "file": File(DEFAULT_FILEPATH),
                },
            ]
        }
    ],
    indirect=True,
    ids=["named_table"],
)
def test_cleanup_non_temp_table(database_table_fixture, tables_fixture):
    db, _ = database_table_fixture
    test_table, test_temp_table = tables_fixture
    assert db.table_exists(test_table)
    assert db.table_exists(test_temp_table)
    test_table.conn_id = db.conn_id
    test_temp_table.conn_id = db.conn_id
    a = aql.cleanup([test_table, test_temp_table])
    a.execute({})
    assert db.table_exists(test_table)
    assert not db.table_exists(test_temp_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_WITH_FILE,
    indirect=True,
    ids=["sqlite", "postgres", "bigquery", "snowflake"],
)
def test_cleanup_non_table(database_table_fixture):
    db, test_table = database_table_fixture
    df = pandas.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    a = aql.cleanup([test_table, df])
    a.execute({})
    assert not db.table_exists(test_table)


@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES,
    indirect=True,
    ids=["sqlite", "postgres", "bigquery", "snowflake"],
)
@pytest.mark.parametrize(
    "tables_fixture",
    [
        {
            "items": [
                {
                    "file": File(DEFAULT_FILEPATH),
                },
                {
                    "file": File(DEFAULT_FILEPATH),
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables"],
)
def test_cleanup_multiple_table(database_table_fixture, tables_fixture):
    db, _ = database_table_fixture
    test_table_1, test_table_2 = tables_fixture
    assert db.table_exists(test_table_1)
    assert db.table_exists(test_table_2)

    df = pandas.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    a = aql.cleanup([test_table_1, test_table_2, df])
    a.execute({})
    assert not db.table_exists(test_table_1)
    assert not db.table_exists(test_table_2)


@pytest.mark.parametrize(
    "database_table_fixture",
    SQLITE_ONLY,
    indirect=True,
    # ids=["sqlite", "postgres", "bigquery", "snowflake"],
)
@pytest.mark.parametrize(
    "tables_fixture",
    [
        {
            "items": [
                {
                    "file": File(DEFAULT_FILEPATH),
                },
                {
                    "file": File(DEFAULT_FILEPATH),
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables"],
)
def test_cleanup_default_all_tables(sample_dag, database_table_fixture, tables_fixture):
    from tests.sql.operators import utils as test_utils

    @aql.transform
    def foo(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    table_1, table_2 = tables_fixture
    with sample_dag:
        foo(table_1)
        foo(table_2)

        aql.cleanup([])
    test_utils.run_dag(sample_dag)


def test_is_dag_running():
    cleanup_op = CleanupOperator(task_id="cleanup")

    task_instances = []
    for i in range(4):
        op = BashOperator(task_id=f"foo_task_{i}", bash_command="")
        ti = TaskInstance(task=op, state=State.SUCCESS)
        task_instances.append(ti)
    assert not cleanup_op._is_dag_running(task_instances=task_instances)
    task_instances[0].state = State.RUNNING
    assert cleanup_op._is_dag_running(task_instances=task_instances)


# def test_filter_for_temp_tables():
#     cleanup_op = CleanupOperator(task_id="cleanup")
#     task_instances = []
#     for i in range(4):
#         op = BashOperator(task_id=f"foo_task_{i}", bash_command="")
#         ti = TaskInstance(task=op, state=State.SUCCESS)
#         task_instances.append(ti)
#     non_temp_task = Tr
