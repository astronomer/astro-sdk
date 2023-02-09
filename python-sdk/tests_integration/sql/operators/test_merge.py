import math
import pathlib
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from airflow.decorators import task_group

from astro import sql as aql
from astro.constants import Database
from astro.databases import create_database
from astro.files import File
from astro.settings import SCHEMA
from astro.table import Metadata, Table

from ..operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


@aql.run_raw_sql
def add_constraint(table: Table, columns):
    db = create_database(table.conn_id)
    return db.get_merge_initialization_query(parameters=columns)


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


@task_group
def run_merge(target_table: Table, source_table: Table, merge_parameters, mode):
    con1 = add_constraint(target_table, merge_parameters["target_conflict_columns"])

    merged_table = aql.merge(
        target_table=target_table,
        source_table=source_table,
        **merge_parameters,
    )
    con1 >> merged_table  # skipcq PYL-W0104
    validate_results(df=merged_table, mode=mode)


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


# TODO: Add DuckDB for this test once https://github.com/astronomer/astro-sdk/issues/1713 is fixed
@pytest.mark.integration
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
    "database_table_fixture",
    [
        {"database": Database.REDSHIFT},
        {"database": Database.POSTGRES},
        {"database": Database.MSSQL},
        {"database": Database.MYSQL},
    ],
    indirect=True,
    ids=["redshift", "postgres", "mssql", "mysql"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {"file": File(str(pathlib.Path(CWD.parent.parent, "data/homes_merge_1.csv")))},
                {"file": File(str(pathlib.Path(CWD.parent.parent, "data/homes_merge_2.csv")))},
            ]
        }
    ],
    indirect=True,
    ids=["two_tables_same_schema"],
)
def test_merge(database_table_fixture, multiple_tables_fixture, sample_dag, merge_parameters):
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


# TODO: Add DuckDB for this test once https://github.com/astronomer/astro-sdk/issues/1713 is fixed
@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.REDSHIFT},
        {"database": Database.DELTA},
        {"database": Database.MYSQL},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "redshift", "delta", "mysql"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {"file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv")))},
                {"file": File(str(pathlib.Path(CWD.parent.parent, "data/sample_part2.csv")))},
            ]
        }
    ],
    indirect=True,
    ids=["two_tables_same_schema"],
)
def test_merge_with_the_same_schema(database_table_fixture, multiple_tables_fixture, sample_dag):
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
    assert first_table.row_count == 4
    test_utils.assert_dataframes_are_equal(computed, expected)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["mssql"],
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {"file": File(str(pathlib.Path(CWD.parent.parent, "data/sample_without_unicode.csv")))},
                {"file": File(str(pathlib.Path(CWD.parent.parent, "data/sample_part2_without_unicode.csv")))},
            ]
        }
    ],
    indirect=True,
    ids=["two_tables_same_schema"],
)
def test_merge_with_the_same_schema_on_mssql(database_table_fixture, multiple_tables_fixture, sample_dag):
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
            {"id": 4, "name": "Czwarte"},
        ]
    )
    assert first_table.row_count == 3
    test_utils.assert_dataframes_are_equal(computed, expected)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.BIGQUERY},
    ],
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
                    "file": File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))),
                },
                {
                    "table": Table(
                        conn_id="bigquery",
                        metadata=Metadata(schema="second_table_schema"),
                    ),
                    "file": File(str(pathlib.Path(CWD.parent.parent, "data/sample_part2.csv"))),
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables"],
)
def test_merge_with_different_schemas(database_table_fixture, multiple_tables_fixture, sample_dag):
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
    assert first_table.row_count == 4
    test_utils.assert_dataframes_are_equal(computed, expected)


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
    f"MERGE {SCHEMA}.target_table T USING {SCHEMA}.source_table S "
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
    f"MERGE {SCHEMA}.target_table T USING {SCHEMA}.source_table S "
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
    f"MERGE {SCHEMA}.target_table T USING {SCHEMA}.source_table S ON T.list=S.list "
    "WHEN NOT MATCHED BY TARGET THEN INSERT (list) VALUES (list)"
)

duckdb_single_result_sql = (
    "INSERT INTO target_table (list) SELECT list FROM source_table "
    "Where true ON CONFLICT (list) DO NOTHING"
)

duckdb_multi_result_sql = (
    "INSERT INTO target_table (list,sell) SELECT list,sell FROM source_table "
    "Where true ON CONFLICT (list,sell) DO NOTHING"
)

duckdb_update_result_sql = (
    "INSERT INTO target_table (list,sell,taxes) SELECT list,sell,age FROM "
    "source_table Where true ON CONFLICT (list,sell) DO UPDATE SET "
    "list=EXCLUDED.list,sell=EXCLUDED.sell,taxes=EXCLUDED.taxes"
)

mysql_single_result_sql = (
    "insert into target_table(list)  select list from source_table  "
    "on duplicate key update target_table.list=source_table.list;"
)

mysql_multi_result_sql = (
    "insert into target_table(list,sell)  select list,sell from source_table  "
    "on duplicate key update target_table.list=source_table.list,target_table.sell=source_table.sell;"
)

mysql_update_result_sql = (
    "insert into target_table(list,sell,taxes)  select list,sell,age from source_table  "
    "on duplicate key update target_table.list=source_table.list,"
    "target_table.sell=source_table.sell,target_table.taxes=source_table.age;"
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
    update_result_sql = {
        "sqlite": sqlite_update_result_sql,
        "snowflake": snowflake_update_result_sql,
        "delta": delta_update_result_sql,
        "bigquery": bigquery_update_result_sql,
        "duckdb": duckdb_update_result_sql,
        "mysql": mysql_update_result_sql,
    }
    return update_result_sql[database_type]


def get_result_sql_multi(conn_id):
    database_type = create_database(conn_id=conn_id).sql_type
    multi_result_sql = {
        "sqlite": sqlite_multi_result_sql,
        "snowflake": snowflake_multi_result_sql,
        "delta": delta_multi_result_sql,
        "bigquery": bigquery_multi_result_sql,
        "duckdb": duckdb_multi_result_sql,
        "mysql": mysql_multi_result_sql,
    }
    return multi_result_sql[database_type]


def get_result_sql_single(conn_id):
    database_type = create_database(conn_id=conn_id).sql_type
    single_result_sql = {
        "sqlite": sqlite_single_result_sql,
        "snowflake": snowflake_single_result_sql,
        "delta": delta_single_result_sql,
        "bigquery": bigquery_single_result_sql,
        "duckdb": duckdb_single_result_sql,
        "mysql": mysql_single_result_sql,
    }
    return single_result_sql[database_type]


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
        (base_database_class, "duckdb_conn"),
        (base_database_class, "mysql_conn"),
    ],
    ids=["sqlite", "bigquery", "snowflake", "databricks", "duckdb", "mysql"],
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
    assert mock_run_sql.call_args_list[0][1]["sql"] == get_result_sql(conn_id, mode)
