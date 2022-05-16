import logging
import pathlib

import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.utils import timezone

import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES
from astro.sql.tables import Table
from tests.operators.utils import run_dag

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_range_values(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(test_table)
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            greater_than=4,
            less_than=4,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_out_of_range_value(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            aggregate_table = get_table(test_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=10,
                less_than=20,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_equal_to_param(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(test_table)
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            equal_to=4,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_only_less_than_param(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            aggregate_table = get_table(test_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                less_than=3,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_only_greater_than_param(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(test_table)
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            greater_than=3,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_all_three_params_provided_priority_given_to_equal_to_param(
    sample_dag, test_table
):
    """greater_than should be less than or equal to less_than"""

    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(ValueError):
        with sample_dag:
            aggregate_table = get_table(test_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=20,
                less_than=10,
                equal_to=4,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_invalid_params_no_test_values(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(ValueError):
        with sample_dag:
            aggregate_table = get_table(test_table)
            aql.aggregate_check(
                table=aggregate_table, check="select count(*) FROM {{table}}"
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes_merge_1.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_invalid_values(sample_dag, test_table):
    """greater_than should be less than or equal to less_than"""

    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(ValueError):
        with sample_dag:
            aggregate_table = get_table(test_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=20,
                less_than=10,
            )
        run_dag(sample_dag)
