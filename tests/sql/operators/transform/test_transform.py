import pathlib

import pandas as pd
import pytest
from airflow.decorators import task

from astro import sql as aql
from astro.constants import Database
from astro.files import File
from astro.sql.table import Table
from tests.sql.operators import utils as test_utils

cwd = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.SQLITE},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite"],
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
        assert df.equals(
            pd.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})
        )

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
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite"],
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
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite"],
)
def test_raw_sql(database_table_fixture, sample_dag):
    _, test_table = database_table_fixture

    @aql.run_raw_sql
    def raw_sql_query(my_input_table: Table, created_table: Table, num_rows: int):
        return "SELECT * FROM {{my_input_table}} LIMIT {{num_rows}}"

    @task
    def validate_raw_sql(cur):
        print(cur)

    with sample_dag:
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
        )
        raw_sql_result = (
            raw_sql_query(
                my_input_table=homes_file,
                created_table=test_table,
                num_rows=5,
                handler=lambda cur: cur.fetchall(),
            ),
        )
        validate_raw_sql(raw_sql_result)
    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
        {"database": Database.BIGQUERY},
        {"database": Database.POSTGRES},
        {"database": Database.SQLITE},
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite"],
)
def test_raw_sql_chained_queries(database_table_fixture, sample_dag):
    import pandas

    db, test_table = database_table_fixture

    @aql.transform(conn_id=db.conn_id)
    def raw_sql_no_deps():
        """
        Let' test without any data dependencies, purely using upstream_tasks
        Returns:

        """
        return f"""SELECT * FROM {test_table.name};"""

    @aql.dataframe
    def validate(df: pandas.DataFrame):
        assert df.columns.tolist() == [
            "sell",
            "list",
            "living",
            "rooms",
            "beds",
            "baths",
            "age",
            "acres",
            "taxes",
        ]

    with sample_dag:
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../../data/homes.csv"),
            output_table=test_table,
        )
        customers = raw_sql_no_deps(upstream_tasks=[homes_file])
        customers1 = raw_sql_no_deps(upstream_tasks=[customers])
        customers2 = raw_sql_no_deps(upstream_tasks=[customers1])
        validate(test_table, upstream_tasks=[customers2])

    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
            "file": File(
                "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv"
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
            SELECT Title, Rating
            FROM {{ input_table }}
            WHERE Genre1=='Animation'
            ORDER BY Rating desc
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
