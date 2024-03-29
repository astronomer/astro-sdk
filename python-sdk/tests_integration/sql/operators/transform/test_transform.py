import os
import pathlib
import tempfile

import pandas as pd
import pytest
from airflow.decorators import task

from astro import sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import Database
from astro.databases.databricks.load_options import DeltaLoadOptions
from astro.files import File
from astro.table import Metadata, Table
from tests.sql.operators import utils as test_utils

cwd = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.SQLITE},
        {"database": Database.REDSHIFT},
        {"database": Database.MSSQL},
        {"database": Database.MYSQL},
        {"database": Database.DUCKDB},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift", "mssql", "mysql", "duckdb"],
)
def test_dataframe_transform(database_table_fixture, sample_dag):
    _, test_table = database_table_fixture

    @aql.dataframe
    def get_dataframe():
        return pd.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})

    @aql.transform
    def sample_pg(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    @aql.dataframe
    def validate_dataframe(df: pd.DataFrame):
        df.columns = df.columns.str.lower()
        df = df.sort_values(by=df.columns.tolist()).reset_index(drop=True)
        assert df.equals(pd.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}))

    with sample_dag:
        my_df = get_dataframe(output_table=test_table)
        pg_df = sample_pg(my_df)
        validate_dataframe(pg_df)
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.SQLITE},
        {"database": Database.REDSHIFT},
        {"database": Database.DELTA},
        {"database": Database.DUCKDB},
        {"database": Database.MYSQL},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift", "delta", "duckdb", "mysql"],
)
def test_transform(database_table_fixture, sample_dag):
    _, test_table = database_table_fixture

    @aql.transform
    def sample_function(input_table: Table):
        return "SELECT * FROM {{input_table}} LIMIT 10"

    @aql.dataframe
    def validate_table(df: pd.DataFrame):
        assert len(df) == 10

    with sample_dag:
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
            load_options=[DeltaLoadOptions.get_default_delta_options()],
        )
        first_model = sample_function(
            input_table=homes_file,
        )
        inherit_model = sample_function(
            input_table=first_model,
        )
        validate_table(inherit_model)
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["mssql"],
)
def test_transform_mssql(database_table_fixture, sample_dag):
    _, test_table = database_table_fixture

    @aql.transform
    def sample_function(input_table: Table):
        return "SELECT TOP 10 * FROM {{input_table}}"

    @aql.dataframe
    def validate_table(df: pd.DataFrame):
        assert len(df) == 10

    with sample_dag:
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
        )
        first_model = sample_function(
            input_table=homes_file,
        )
        inherit_model = sample_function(
            input_table=first_model,
        )
        validate_table(inherit_model)
    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.SQLITE},
        {"database": Database.REDSHIFT},
        {"database": Database.DELTA},
        {"database": Database.DUCKDB},
        {"database": Database.MYSQL},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift", "delta", "duckdb", "mysql"],
)
def test_raw_sql(database_table_fixture, sample_dag):
    db, test_table = database_table_fixture

    @aql.run_raw_sql
    def raw_sql_query(my_input_table: Table, created_table: Table, num_rows: int):
        return "SELECT * FROM {{my_input_table}} LIMIT {{num_rows}}"

    @task
    def validate_raw_sql(cur: pd.DataFrame):
        from sqlalchemy.engine.row import LegacyRow

        if db.sql_type == "delta":
            for c in cur:
                assert isinstance(c, list)
        else:
            for c in cur:
                assert isinstance(c, LegacyRow)

    with sample_dag:
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
            load_options=[DeltaLoadOptions.get_default_delta_options()],
        )
        raw_sql_result = raw_sql_query(
            my_input_table=homes_file,
            created_table=test_table,
            num_rows=5,
            handler=lambda cur: cur.fetchall(),
        )
        validate_raw_sql(raw_sql_result)
    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.MSSQL},
    ],
    indirect=True,
    ids=["mssql"],
)
def test_raw_sql_for_mssql(database_table_fixture, sample_dag):
    db, test_table = database_table_fixture

    @aql.run_raw_sql
    def raw_sql_query(my_input_table: Table, created_table: Table, num_rows: int):
        return "SELECT TOP {{num_rows}} * FROM {{my_input_table}}"

    @task
    def validate_raw_sql(cur: pd.DataFrame):
        from sqlalchemy.engine.row import LegacyRow

        if db.sql_type == "delta":
            for c in cur:
                assert isinstance(c, list)
        else:
            for c in cur:
                assert isinstance(c, LegacyRow)

    with sample_dag:
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
        )
        raw_sql_result = raw_sql_query(
            my_input_table=homes_file,
            created_table=test_table,
            num_rows=5,
            handler=lambda cur: cur.fetchall(),
        )
        validate_raw_sql(raw_sql_result)
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="sqlite_default"),
        },
        {
            "database": Database.DUCKDB,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="duckdb_conn"),
        },
    ],
    indirect=True,
    ids=["sqlite", "duckdb"],
)
def test_transform_with_templated_table_name(database_table_fixture, sample_dag):
    """Test table creation via select statement when the output table uses an Airflow template in its name"""
    database, imdb_table = database_table_fixture

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return """
            SELECT title, rating
            FROM {{ input_table }}
            WHERE genre1=='Animation'
            ORDER BY rating desc
            LIMIT 5;
        """

    with sample_dag:
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id=imdb_table.conn_id)

        top_five_animations(input_table=imdb_table, output_table=target_table)
    test_utils.run_dag(sample_dag)

    expected_target_table = target_table.create_similar_table()
    expected_target_table.name = "test_is_True"
    database.drop_table(expected_target_table)
    assert not database.table_exists(expected_target_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(conn_id="sqlite_default"),
        },
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_transform_astro_data_team(database_table_fixture, sample_dag):
    """Test case that represents a usage from the Astronomer Data team.
    aql.ransform is used both receiving a SQL string and also as a file path."""
    _, imdb_table = database_table_fixture

    def query(sql: str) -> Table:
        """
        Takes sql or sql file path and returns result as a Table.

        """
        return sql

    sample_query = f"""
        SELECT title, rating
        FROM {imdb_table.name}
        WHERE genre1=='Animation'
        ORDER BY rating desc
        LIMIT 5;
    """

    with tempfile.NamedTemporaryFile(mode="w", suffix=".sql") as temp_file:
        temp_file.write(sample_query)
        temp_file.flush()

        with sample_dag:
            aql.transform(
                conn_id="sqlite_default",
                task_id="table_from_inline_sql",
            )(
                query
            )(sql=sample_query)

            aql.transform(
                conn_id="sqlite_default",
                task_id="table_from_sql_file",
            )(
                query
            )(sql=temp_file.name)

        test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.MYSQL,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="mysql_conn"),
        },
    ],
    indirect=True,
    ids=["mysql"],
)
def test_transform_with_templated_table_for_mysql(database_table_fixture, sample_dag):
    """Test table creation via select statement when the output table uses an Airflow template in its name"""
    database, imdb_table = database_table_fixture

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return """
            SELECT title, rating
            FROM {{ input_table }}
            WHERE genre1='Animation'
            ORDER BY rating desc
            LIMIT 5;
        """

    with sample_dag:
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id=imdb_table.conn_id)

        top_five_animations(input_table=imdb_table, output_table=target_table)
    test_utils.run_dag(sample_dag)

    expected_target_table = target_table.create_similar_table()
    expected_target_table.name = "test_is_True"
    database.drop_table(expected_target_table)
    assert not database.table_exists(expected_target_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.MSSQL,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="mssql_conn"),
        }
    ],
    indirect=True,
    ids=["mssql"],
)
def test_transform_with_templated_table_name_for_mssql(database_table_fixture, sample_dag):
    """Test table creation via select statement when the output table uses an Airflow template in its name"""
    database, imdb_table = database_table_fixture

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        # Don't use quote at the end of the query here due to below error
        # sqlalchemy.exc.ProgrammingError: (pymssql._pymssql.ProgrammingError) (102, b"Incorrect syntax near ';'
        # use TOP as LIMIT does not work in mssql
        return """
            SELECT TOP 5 title, rating
            FROM {{ input_table }}
            WHERE genre1='Animation'
            ORDER BY rating desc
        """

    with sample_dag:
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id="mssql_conn")

        top_five_animations(input_table=imdb_table, output_table=target_table)
    test_utils.run_dag(sample_dag)

    expected_target_table = target_table.create_similar_table()
    expected_target_table.name = "test_is_True"
    database.drop_table(expected_target_table)
    assert not database.table_exists(expected_target_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="sqlite_default"),
        },
        {
            "database": Database.DUCKDB,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="duckdb_conn"),
        },
    ],
    indirect=True,
    ids=["sqlite", "duckdb"],
)
def test_transform_with_file(database_table_fixture, sample_dag):
    """Test table creation via select statement in a SQL file"""
    database, imdb_table = database_table_fixture

    @aql.dataframe
    def validate(df: pd.DataFrame):
        assert df.columns.tolist() == ["title", "rating"]

    with sample_dag:
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id=imdb_table.conn_id)
        table_from_query = aql.transform_file(
            file_path="tests_integration/sql/operators/transform/test.sql",
            parameters={"input_table": imdb_table},
            op_kwargs={"output_table": target_table},
        )
        validate(table_from_query)
    test_utils.run_dag(sample_dag)

    expected_target_table = target_table.create_similar_table()
    expected_target_table.name = "test_is_True"
    database.drop_table(expected_target_table)
    assert not database.table_exists(expected_target_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.MSSQL,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="mssql_conn"),
        },
    ],
    indirect=True,
    ids=["mssql"],
)
def test_transform_with_file_for_mssql(database_table_fixture, sample_dag):
    """Test table creation via select statement in a SQL file"""
    database, imdb_table = database_table_fixture

    @aql.dataframe
    def validate(df: pd.DataFrame):
        assert df.columns.tolist() == ["title", "rating"]

    with sample_dag:
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id="mssql_conn")
        table_from_query = aql.transform_file(
            file_path="tests_integration/sql/operators/transform/test_mssql.sql",
            parameters={"input_table": imdb_table},
            op_kwargs={"output_table": target_table},
        )
        validate(table_from_query)
    test_utils.run_dag(sample_dag)

    expected_target_table = target_table.create_similar_table()
    expected_target_table.name = "test_is_True"
    database.drop_table(expected_target_table)
    assert not database.table_exists(expected_target_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.MYSQL,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
            "table": Table(name="imdb", conn_id="mysql_conn"),
        },
    ],
    indirect=True,
    ids=["mysql"],
)
def test_transform_with_file_for_mysql(database_table_fixture, sample_dag):
    """Test table creation via select statement in a SQL file"""
    database, imdb_table = database_table_fixture

    @aql.dataframe
    def validate(df: pd.DataFrame):
        assert df.columns.tolist() == ["title", "rating"]

    with sample_dag:
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id="mysql_conn")
        table_from_query = aql.transform_file(
            file_path="tests_integration/sql/operators/transform/test_mysql.sql",
            parameters={"input_table": imdb_table},
            op_kwargs={"output_table": target_table},
        )
        validate(table_from_query)
    test_utils.run_dag(sample_dag)

    expected_target_table = target_table.create_similar_table()
    expected_target_table.name = "test_is_True"
    database.drop_table(expected_target_table)
    assert not database.table_exists(expected_target_table)


def test_transform_using_table_metadata(sample_dag):
    """
    Test that load file and transform when database and schema is available in table metadata instead of conn
    """
    with sample_dag:
        test_table = Table(
            conn_id="snowflake_conn_1",
            metadata=Metadata(
                database=os.environ["SNOWFLAKE_DATABASE"],
                schema=os.environ["SNOWFLAKE_SCHEMA"],
            ),
        )
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
        )

        @aql.transform
        def select(input_table: Table):
            return "SELECT * FROM {{input_table}} LIMIT 4;"

        select(input_table=homes_file, output_table=Table(conn_id="snowflake_conn_1"))
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
def test_transform_using_table_metadata_mssql(sample_dag):
    """
    Test that load file and transform work when database and schema is available in table metadata instead of conn
    """
    with sample_dag:
        test_table = Table(
            conn_id="mssql_conn",
            metadata=Metadata(
                database=os.environ["MSSQL_DB"],
                schema="dbo",
            ),
        )
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
        )

        @aql.transform
        def select(input_table: Table):
            return "SELECT TOP 4 * FROM {{input_table}}"

        select(input_table=homes_file, output_table=Table(conn_id="mssql_conn"))
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
def test_transform_using_table_metadata_mysql(sample_dag):
    """
    Test that load file and transform work when schema is available in table metadata instead of conn
    Note that schema is synonymous with database in mysql
    """
    with sample_dag:
        test_table = Table(
            conn_id="mysql_conn",
            metadata=Metadata(
                schema=os.environ["MYSQL_DB"],
            ),
        )
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
        )

        @aql.transform
        def select(input_table: Table):
            return "SELECT * FROM {{input_table}} LIMIT 4"

        select(input_table=homes_file, output_table=Table(conn_id="mysql_conn"))
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
def test_cross_db_transform_raise_exception(sample_dag):
    """Test the transform operator raise exception if input and output is not for same database source"""

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return """
            SELECT title, rating
            FROM {{ input_table }}
            WHERE genre1=='Animation'
            ORDER BY rating desc
            LIMIT 5;
        """

    with sample_dag:
        input_table = Table(conn_id="snowflake_conn", name="test1", metadata=Metadata(schema="test"))
        output_table = Table(conn_id="bigquery", name="test2", metadata=Metadata(schema="test"))

        top_five_animations(input_table=input_table, output_table=output_table)
    with pytest.raises(ValueError) as exec_info:
        test_utils.run_dag(sample_dag)
    assert exec_info.value.args[0] == "source and target table must belong to the same datasource"


@pytest.mark.integration
def test_transform_region(sample_dag):
    """Test the transform operator raise exception if input and output is not for same database source"""

    @aql.transform
    def select_all(input_table: Table) -> str:
        return """
            SELECT *
            FROM {{ input_table }}
        """

    with sample_dag:
        input_table = Table(
            conn_id="google_cloud_default", name="do_not_delete", metadata=Metadata(schema="testing_region")
        )
        select_all(input_table=input_table)
        aql.cleanup()
    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.skipif(not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    imdb_table = (Table(name="imdb", conn_id="sqlite_default"),)
    output_table = Table(name="test_name")

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return "SELECT title, rating FROM {{ input_table }} LIMIT 5;"

    task = top_five_animations(input_table=imdb_table, output_table=output_table)
    assert task.operator.outlets == [output_table]


@pytest.mark.integration
@pytest.mark.skipif(not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_supported_ds_mssql():
    """Test Datasets are set as inlets and outlets"""
    imdb_table = (Table(name="imdb", conn_id="mssql_conn"),)
    output_table = Table(name="test_name")

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return "SELECT TOP 5 title, rating FROM {{ input_table }}"

    task = top_five_animations(input_table=imdb_table, output_table=output_table)
    assert task.operator.outlets == [output_table]


@pytest.mark.integration
@pytest.mark.skipif(DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    imdb_table = (Table(name="imdb", conn_id="sqlite_default"),)
    output_table = Table(name="test_name")

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return "SELECT title, rating FROM {{ input_table }} LIMIT 5;"

    task = top_five_animations(input_table=imdb_table, output_table=output_table)
    assert task.operator.outlets == []


@pytest.mark.integration
@pytest.mark.skipif(DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_non_supported_ds_mssql():
    """Test inlets and outlets are not set if Datasets are not supported"""
    imdb_table = (Table(name="imdb", conn_id="mssql_conn"),)
    output_table = Table(name="test_name")

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return "SELECT TOP 5 title, rating FROM {{ input_table }}"

    task = top_five_animations(input_table=imdb_table, output_table=output_table)
    assert task.operator.outlets == []
