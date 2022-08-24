import math
import os
import pathlib
from unittest import mock

import pandas as pd
import pytest
from airflow.decorators import task_group
from astro import sql as aql
from astro.constants import Database
from astro.databases import create_database
from astro.files import File
from astro.sql import MergeOperator
from astro.sql.table import Metadata, Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


@pytest.fixture
def merge_parameters(request):
    mode = request.param
    if mode == "single":
        return (
            {
                "target_conflict_columns": ["list"],
                "columns": {"list": "list"},
                "if_conflicts": "ignore",
            },
            mode,
        )
    elif mode == "multi":
        return (
            {
                "target_conflict_columns": ["list", "sell"],
                "columns": {"list": "list", "sell": "sell"},
                "if_conflicts": "ignore",
            },
            mode,
        )
    # elif mode == "update":
    return (
        {
            "target_conflict_columns": ["list", "sell"],
            "columns": {
                "list": "list",
                "sell": "sell",
                "age": "taxes",
            },
            "if_conflicts": "update",
        },
        mode,
    )


@aql.run_raw_sql
def add_constraint(table: Table, columns):
    db = create_database(table.conn_id)
    return db.get_merge_initialization_query(parameters=columns)


@aql.dataframe
def validate_results(df: pd.DataFrame, mode):
    def set_compare(l1, l2):
        l1 = list(filter(lambda val: not math.isnan(val), l1))
        return set(l1) == set(l2)

    df = df.sort_values(by=["list"], ascending=True)

    if mode == "single":
        assert set_compare(df.age.to_list()[:-1], [60.0, 12.0, 41.0, 22.0])
        assert set_compare(df.taxes.to_list()[:-1], [3167.0, 4033.0, 1471.0, 3204.0])
        assert set_compare(df.list.to_list(), [160, 180, 132, 140, 240])
        assert set_compare(df.sell.to_list()[:-1], [142, 175, 129, 138])
    elif mode == "multi":
        assert set_compare(df.age.to_list()[:-1], [60.0, 12.0, 41.0, 22.0])
        assert set_compare(df.taxes.to_list()[:-1], [3167.0, 4033.0, 1471.0, 3204.0])
        assert set_compare(df.list.to_list(), [160, 180, 132, 140, 240])
        assert set_compare(df.sell.to_list()[:-1], [142, 175, 129, 138])
    elif mode == "update":
        assert df.taxes.to_list() == [1, 1, 1, 1, 1]
        assert set_compare(df.age.to_list()[:-1], [60.0, 12.0, 41.0, 22.0])


@task_group
def run_merge(target_table: Table, source_table: Table, merge_parameters, mode):
    con1 = add_constraint(target_table, merge_parameters["target_conflict_columns"])

    merged_table = aql.merge(
        target_table=target_table,
        source_table=source_table,
        **merge_parameters,
    )
    con1 >> merged_table
    validate_results(df=merged_table, mode=mode)


@pytest.mark.integration
@pytest.mark.parametrize(
    "merge_parameters",
    [
        # "None",
        "single",
        "multi",
        "update",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
    ],
    indirect=True,
    ids=["snowflake", "bigquery"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {
                    "file": File(
                        str(pathlib.Path(CWD.parent.parent, "data/homes_merge_1.csv"))
                    )
                },
                {
                    "file": File(
                        str(pathlib.Path(CWD.parent.parent, "data/homes_merge_2.csv"))
                    )
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables_same_schema"],
)
def test_merge(
    database_table_fixture, multiple_tables_fixture, sample_dag, merge_parameters
):
    target_table, merge_table = multiple_tables_fixture
    merge_params, mode = merge_parameters
    with sample_dag:
        run_merge(
            target_table=target_table,
            source_table=merge_table,
            merge_parameters=merge_params,
            mode=mode,
        )
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
    ],
    indirect=True,
    ids=["snowflake", "bigquery"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {"file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv")))},
                {
                    "file": File(
                        str(pathlib.Path(CWD.parent.parent, "data/sample_part2.csv"))
                    )
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables_same_schema"],
)
def test_merge_with_the_same_schema(
    database_table_fixture, multiple_tables_fixture, sample_dag
):
    """
    Validate that the output of merge is what we expect.
    """
    database, _ = database_table_fixture
    first_table, second_table = multiple_tables_fixture

    with sample_dag:
        aql.merge(
            target_table=first_table,
            source_table=second_table,
            target_conflict_columns=["id"],
            columns={"id": "id", "name": "name"},
            if_conflicts="update",
        )

    test_utils.run_dag(sample_dag)
    computed = database.export_table_to_pandas_dataframe(first_table)
    computed = computed.sort_values(by="id", ignore_index=True)
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
            {"id": 4, "name": "Czwarte imię"},
        ]
    )

    test_utils.assert_dataframes_are_equal(computed, expected)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.BIGQUERY}],
    indirect=True,
    ids=["bigquery"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {
                    "table": Table(
                        conn_id="bigquery",
                        metadata=Metadata(schema="first_table_schema"),
                    ),
                    "file": File(
                        str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))
                    ),
                },
                {
                    "table": Table(
                        conn_id="bigquery",
                        metadata=Metadata(schema="second_table_schema"),
                    ),
                    "file": File(
                        str(pathlib.Path(CWD.parent.parent, "data/sample_part2.csv"))
                    ),
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables"],
)
def test_merge_with_different_schemas(
    database_table_fixture, multiple_tables_fixture, sample_dag
):
    """
    Validate that the output of merge is what we expect.
    """
    database, _ = database_table_fixture
    first_table, second_table = multiple_tables_fixture

    with sample_dag:
        aql.merge(
            target_table=first_table,
            source_table=second_table,
            target_conflict_columns=["id"],
            columns={"id": "id", "name": "name"},
            if_conflicts="update",
        )

    test_utils.run_dag(sample_dag)
    computed = database.export_table_to_pandas_dataframe(first_table)
    computed = computed.sort_values(by="id", ignore_index=True)
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
            {"id": 4, "name": "Czwarte imię"},
        ]
    )

    test_utils.assert_dataframes_are_equal(computed, expected)


@pytest.mark.parametrize(
    "test_columns,expected_columns",
    [
        (["sell", "list"], {"sell": "sell", "list": "list"}),
        (("sell", "list"), {"sell": "sell", "list": "list"}),
        (
            {"s_sell": "t_sell", "s_list": "t_list"},
            {"s_sell": "t_sell", "s_list": "t_list"},
        ),
    ],
)
def test_columns_params(test_columns, expected_columns):
    """
    Test that the columns param in MergeOperator takes list/tuple/dict and converts them to dict
    before sending over to db.merge_table()
    """
    source_table = Table(
        name="source_table", conn_id="test1", metadata=Metadata(schema="test")
    )
    target_table = Table(
        name="target_table", conn_id="test2", metadata=Metadata(schema="test")
    )
    merge_task = MergeOperator(
        source_table=source_table,
        target_table=target_table,
        if_conflicts="ignore",
        target_conflict_columns=["list"],
        columns=test_columns,
    )
    assert merge_task.columns == expected_columns
    with mock.patch(
        "astro.databases.sqlite.SqliteDatabase.merge_table"
    ) as mock_merge, mock.patch.dict(
        os.environ,
        {"AIRFLOW_CONN_TEST1": "sqlite://", "AIRFLOW_CONN_TEST2": "sqlite://"},
    ):
        merge_task.execute({})
        mock_merge.assert_called_once_with(
            source_table=source_table,
            target_table=target_table,
            if_conflicts="ignore",
            target_conflict_columns=["list"],
            source_to_target_columns_map=expected_columns,
        )


def test_invalid_columns_param():
    """Test that an error is raised when an invalid columns type is passed"""
    source_table = Table(
        name="source_table", conn_id="test1", metadata=Metadata(schema="test")
    )
    target_table = Table(
        name="target_table", conn_id="test2", metadata=Metadata(schema="test")
    )
    with pytest.raises(ValueError) as exec_info:
        MergeOperator(
            source_table=source_table,
            target_table=target_table,
            if_conflicts="ignore",
            target_conflict_columns=["list"],
            columns={"set_item_1", "set_item_2", "set_item_3"},
        )
    assert (
        exec_info.value.args[0]
        == "columns is not a valid type. Valid types: [tuple, list, dict], Passed: <class 'set'>"
    )
