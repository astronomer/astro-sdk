import os
import pathlib

import pandas as pd
import pytest
from airflow.decorators import task

from astro import sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import Database
from astro.databricks.load_options import DeltaLoadOptions
from astro.files import File
from astro.table import Metadata, Table
from tests.sql.operators import utils as test_utils

cwd = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.SQLITE},
        {"database": Database.REDSHIFT},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
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
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift", "delta"],
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
            load_options=DeltaLoadOptions.get_default_delta_options(),
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
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift", "delta"],
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
            load_options=DeltaLoadOptions.get_default_delta_options(),
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
        }
    ],
    indirect=True,
    ids=["sqlite"],
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
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id="sqlite_default")

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
        }
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_transform_with_file(database_table_fixture, sample_dag):
    """Test table creation via select statement in a SQL file"""
    database, imdb_table = database_table_fixture

    @aql.dataframe
    def validate(df: pd.DataFrame):
        assert df.columns.tolist() == ["title", "rating"]

    with sample_dag:
        target_table = Table(name="test_is_{{ ds_nodash }}", conn_id="sqlite_default")
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
