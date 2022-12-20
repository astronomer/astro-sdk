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
            "file": File(path=str(CWD) + "/../../../data/homes_main.csv"),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(path=str(CWD) + "/../../../data/homes_main.csv"),
            "table": Table(conn_id="gcp_conn_project"),
        },
        {
            "database": Database.POSTGRES,
            "file": File(path=str(CWD) + "/../../../data/homes_main.csv"),
        },
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../../data/homes_main.csv"),
        },
        {
            "database": Database.REDSHIFT,
            "file": File(path=str(CWD) + "/../../../data/homes_main.csv"),
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
    db, test_table = database_table_fixture
    with sample_dag:
        aql.SQLCheckOperator(
            dataset=test_table,
            checks={
                "sell_list": {"check_statement": "sell <= list"},
            },
        )
    test_utils.run_dag(sample_dag)
