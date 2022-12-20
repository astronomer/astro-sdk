import pathlib

import airflow
import pandas
import pytest

import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES, Database
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
def test_cleanup_one_table(database_table_fixture):
    db, test_table = database_table_fixture
    assert db.table_exists(test_table)
    a = aql.cleanup([test_table])
    a.execute({})
    assert not db.table_exists(test_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
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
def test_cleanup_non_temp_table(database_table_fixture, multiple_tables_fixture):
    db, _ = database_table_fixture
    test_table, test_temp_table = multiple_tables_fixture
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
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
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
def test_cleanup_multiple_table(database_table_fixture, multiple_tables_fixture):
    db, _ = database_table_fixture
    test_table_1, test_table_2 = multiple_tables_fixture
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {"file": File(path=DEFAULT_FILEPATH)},
                {
                    "file": File(DEFAULT_FILEPATH),
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables"],
)
def test_cleanup_default_all_tables(sample_dag, database_table_fixture, multiple_tables_fixture):
    @aql.transform()
    def foo(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    db, _ = database_table_fixture
    table_1, table_2 = multiple_tables_fixture
    assert db.table_exists(table_1)
    assert db.table_exists(table_2)

    with sample_dag:
        foo(table_1, output_table=table_2)

        aql.cleanup()
    test_utils.run_dag(sample_dag)

    assert not db.table_exists(table_2)


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
