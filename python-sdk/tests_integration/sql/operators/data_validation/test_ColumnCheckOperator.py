import pathlib

import pytest

from astro import sql as aql
from astro.constants import Database
from astro.files import File
from astro.table import Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "file": File(path=str(CWD) + "/../../../data/data_validation.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../../data/data_validation.csv"),
            "table": Table(conn_id="gcp_conn_project"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../../data/data_validation.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../../data/data_validation.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../../data/data_validation.csv"),
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_column_check_operator_with_table_dataset(sample_dag, database_table_fixture):
    """
    Test column_check_operator with table dataset for all checks types and make sure the generated sql is working for
    all the database we support.
    """
    _, test_table = database_table_fixture
    with sample_dag:
        aql.ColumnCheckOperator(
            dataset=test_table,
            column_mapping={
                "name": {
                    "null_check": {"geq_to": 0, "leq_to": 1},
                    "unique_check": {
                        "equal_to": 0,
                    },
                },
                "city": {
                    "distinct_check": {"geq_to": 2, "leq_to": 3},  # Nulls are treated as values
                },
                "age": {
                    "max": {
                        "leq_to": 100,
                    },
                },
                "emp_id": {
                    "min": {
                        "geq_to": 1,
                    }
                },
            },
        )
    test_utils.run_dag(sample_dag)
