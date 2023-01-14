import os
import pathlib
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest

from astro import sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.databases import create_database
from astro.sql import MergeOperator
from astro.table import Metadata, Table
from tests.utils.airflow import create_context

CWD = pathlib.Path(__file__).parent


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
    source_table = Table(name="source_table", conn_id="test1", metadata=Metadata(schema="test"))
    target_table = Table(name="target_table", conn_id="test2", metadata=Metadata(schema="test"))
    merge_task = MergeOperator(
        source_table=source_table,
        target_table=target_table,
        if_conflicts="ignore",
        target_conflict_columns=["list"],
        columns=test_columns,
    )
    assert merge_task.columns == expected_columns
    with mock.patch("astro.databases.sqlite.SqliteDatabase.merge_table") as mock_merge, mock.patch.dict(
        os.environ,
        {"AIRFLOW_CONN_TEST1": "sqlite://", "AIRFLOW_CONN_TEST2": "sqlite://"},
    ):
        merge_task.execute(context=create_context(merge_task))
        mock_merge.assert_called_once_with(
            source_table=source_table,
            target_table=target_table,
            if_conflicts="ignore",
            target_conflict_columns=["list"],
            source_to_target_columns_map=expected_columns,
        )


def test_invalid_columns_param():
    """Test that an error is raised when an invalid columns type is passed"""
    source_table = Table(name="source_table", conn_id="test1", metadata=Metadata(schema="test"))
    target_table = Table(name="target_table", conn_id="test2", metadata=Metadata(schema="test"))
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


@pytest.mark.skipif(not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    source_table = Table(name="source_table", conn_id="test1", metadata=Metadata(schema="test"))
    target_table = Table(name="target_table", conn_id="test2", metadata=Metadata(schema="test"))
    task = aql.merge(
        source_table=source_table,
        target_table=target_table,
        target_conflict_columns=["list"],
        columns=["set_item_1", "set_item_2", "set_item_3"],
        if_conflicts="ignore",
    )
    assert task.operator.inlets == [source_table]
    assert task.operator.outlets == [target_table]


@pytest.mark.skipif(DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    source_table = Table(name="source_table", conn_id="test1", metadata=Metadata(schema="test"))
    target_table = Table(name="target_table", conn_id="test2", metadata=Metadata(schema="test"))
    task = aql.merge(
        source_table=source_table,
        target_table=target_table,
        target_conflict_columns=["list"],
        columns=["set_item_1", "set_item_2", "set_item_3"],
        if_conflicts="ignore",
    )
    assert task.operator.inlets == []
    assert task.operator.outlets == []


def test_cross_db_merge_raise_exception(monkeypatch):
    monkeypatch.setenv("AIRFLOW_CONN_SNOWFLAKE_CONN", "snowflake://")
    monkeypatch.setenv("AIRFLOW_CONN_BIGQUERY", "bigquery://")

    source_table = Table(conn_id="snowflake_conn", name="test1", metadata=Metadata(schema="test"))
    target_table = Table(conn_id="bigquery", name="test2", metadata=Metadata(schema="test"))
    with pytest.raises(ValueError) as exec_info:
        MergeOperator(
            source_table=source_table,
            target_table=target_table,
            if_conflicts="ignore",
            target_conflict_columns=["list"],
            columns=["set_item_1", "set_item_2", "set_item_3"],
        ).execute({})
    assert exec_info.value.args[0] == "source and target table must belong to the same datasource"


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


sqlite_update_result_sql = (
    "INSERT INTO target_table (list,sell,taxes) SELECT list,sell,age FROM source_table "
    "Where true ON CONFLICT (list,sell) DO UPDATE SET "
    "list=EXCLUDED.list,sell=EXCLUDED.sell,taxes=EXCLUDED.taxes"
)

snowflake_update_result_sql = (
    "merge into IDENTIFIER(:target_table) using IDENTIFIER(:source_table) "
    "on Identifier(:merge_clause_target_0)=Identifier(:merge_clause_source_0) "
    "AND Identifier(:merge_clause_target_1)=Identifier(:merge_clause_source_1) "
    "when matched then "
    "UPDATE SET "
    "target_table.list=source_table.list,target_table.sell=source_table.sell,"
    "target_table.taxes=source_table.age "
    "when not matched then insert(target_table.list,target_table.sell,target_table.taxes) "
    "values (source_table.list,source_table.sell,source_table.age)"
)

delta_update_result_sql = (
    "merge into target_table as `target_table` using source_table as"
    " `source_table` on `target_table`.`list`=`source_table`.`list` AND "
    "`target_table`.`sell`=`source_table`.`sell` "
    "when matched then UPDATE SET target_table.list = source_table.list,"
    "target_table.sell = source_table.sell,target_table.taxes = source_table.age "
    "when not matched then insert(target_table.list,target_table.sell,target_table.taxes) "
    "values (source_table.list,source_table.sell,source_table.age)"
)
bigquery_update_result_sql = (
    "MERGE tmp_astro.target_table T USING tmp_astro.source_table S "
    "ON T.list=S.list AND T.sell=S.sell WHEN NOT MATCHED BY TARGET THEN INSERT "
    "(list, sell, taxes) VALUES (list, sell, age) WHEN MATCHED THEN UPDATE SET T.list=S.list, "
    "T.sell=S.sell, T.taxes=S.age"
)

sqlite_multi_result_sql = (
    "INSERT INTO target_table (list,sell) SELECT list,sell FROM source_table "
    "Where true ON CONFLICT (list,sell) DO NOTHING"
)
snowflake_multi_result_sql = (
    "merge into IDENTIFIER(:target_table) using IDENTIFIER(:source_table) on "
    "Identifier(:merge_clause_target_0)=Identifier(:merge_clause_source_0) AND "
    "Identifier(:merge_clause_target_1)=Identifier(:merge_clause_source_1) "
    "when not matched then insert(target_table.list,target_table.sell) values "
    "(source_table.list,source_table.sell)"
)
delta_multi_result_sql = (
    "merge into target_table as `target_table` using source_table as `source_table` on "
    "`target_table`.`list`=`source_table`.`list` AND `target_table`.`sell`=`source_table`.`sell`"
    " when not matched then insert(target_table.list,target_table.sell) "
    "values (source_table.list,source_table.sell)"
)
bigquery_multi_result_sql = (
    "MERGE tmp_astro.target_table T USING tmp_astro.source_table S "
    "ON T.list=S.list AND T.sell=S.sell WHEN NOT MATCHED BY TARGET THEN INSERT"
    " (list, sell) VALUES (list, sell)"
)
sqlite_single_result_sql = (
    "INSERT INTO target_table (list) SELECT list "
    "FROM source_table Where true ON CONFLICT (list) DO NOTHING"
)
snowflake_single_result_sql = (
    "merge into IDENTIFIER(:target_table) using IDENTIFIER(:source_table) "
    "on Identifier(:merge_clause_target_0)=Identifier(:merge_clause_source_0)"
    " when not matched then insert(target_table.list) values (source_table.list)"
)
delta_single_result_sql = (
    "merge into target_table as `target_table` using source_table as `source_table`"
    " on `target_table`.`list`=`source_table`.`list` when not matched then "
    "insert(target_table.list) values (source_table.list)"
)
bigquery_single_result_sql = (
    "MERGE tmp_astro.target_table T USING tmp_astro.source_table S ON T.list=S.list "
    "WHEN NOT MATCHED BY TARGET THEN INSERT (list) VALUES (list)"
)

base_database_class = "astro.databases.base.BaseDatabase.run_sql"
delta_database_class = "astro.databases.databricks.delta.DeltaDatabase.run_sql"


def get_result_sql(conn_id, mode):
    if mode == "update":
        return get_result_sql_update(conn_id)
    elif mode == "single":
        return get_result_sql_single(conn_id)
    return get_result_sql_multi(conn_id)


def get_result_sql_update(conn_id):
    database_type = create_database(conn_id=conn_id).sql_type
    if database_type == "sqlite":
        return sqlite_update_result_sql
    elif database_type == "snowflake":
        return snowflake_update_result_sql
    elif database_type == "delta":
        return delta_update_result_sql


def get_result_sql_multi(conn_id):
    database_type = create_database(conn_id=conn_id).sql_type
    if database_type == "sqlite":
        return sqlite_multi_result_sql
    elif database_type == "snowflake":
        return snowflake_multi_result_sql
    elif database_type == "delta":
        return delta_multi_result_sql


def get_result_sql_single(conn_id):
    database_type = create_database(conn_id=conn_id).sql_type
    if database_type == "sqlite":
        return sqlite_single_result_sql
    elif database_type == "snowflake":
        return snowflake_single_result_sql
    elif database_type == "delta":
        return delta_single_result_sql


@pytest.mark.parametrize(
    "merge_parameters",
    [
        "single",
        "multi",
        "update",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "database_class,conn_id",
    [
        (base_database_class, "sqlite_conn"),
        (base_database_class, "gcp_conn"),
        (base_database_class, "snowflake_conn"),
        (delta_database_class, "databricks_conn"),
    ],
    ids=["sqlite", "bigquery", "snowflake", "databricks"],
)
def test_merge_sql_generation(database_class, conn_id, merge_parameters):
    parameters, mode = merge_parameters
    target_table = Table("target_table", conn_id=conn_id)
    source_table = Table("source_table", conn_id=conn_id)
    target_conflict_columns = parameters["target_conflict_columns"]
    columns = parameters["columns"]
    if_conflicts = parameters["if_conflicts"]
    with patch(database_class) as mock_run_sql, patch(
        "astro.databases.google.bigquery.BigqueryDatabase.columns_exist"
    ):
        merge_func = aql.merge(
            target_table=target_table,
            source_table=source_table,
            target_conflict_columns=target_conflict_columns,
            columns=columns,
            if_conflicts=if_conflicts,
        )
        merge_func.operator.execute(MagicMock())
    print(mock_run_sql.call_args_list[0][1]["sql"])
    assert mock_run_sql.call_args_list[0][1]["sql"] == get_result_sql(conn_id, mode)
