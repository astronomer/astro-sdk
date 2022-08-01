import pathlib

import pytest

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
def test_raw_sql_chained_queries(database_table_fixture, sample_dag):
    import pandas

    db, test_table = database_table_fixture

    @aql.run_raw_sql(conn_id=db.conn_id)
    def raw_sql_no_deps(new_table: Table, t_table: Table):
        """
        Let' test without any data dependencies, purely using upstream_tasks
        Returns:

        """
        return "CREATE TABLE {{new_table}} AS SELECT * FROM {{t_table}}"

    @aql.dataframe
    def validate(df1: pandas.DataFrame, df2: pandas.DataFrame):
        print(df1)
        print(df2)
        assert df1.equals(df2)

    with sample_dag:
        homes_file = aql.load_file(
            input_file=File(path=str(cwd) + "/../../data/homes.csv"),
            output_table=test_table,
        )
        generated_tables = []
        last_task = homes_file
        for i in range(5):
            n_table = test_table.create_similar_table()
            n_task = raw_sql_no_deps(
                new_table=n_table, t_table=test_table, upstream_tasks=[last_task]
            )
            generated_tables.append(n_table)
            last_task = n_task

        validated = validate(
            df1=test_table, df2=generated_tables[-1], upstream_tasks=[last_task]
        )
        for table in generated_tables:
            aql.drop_table(table, upstream_tasks=[validated])

    test_utils.run_dag(sample_dag)
    all_tasks = sample_dag.tasks
    for t in all_tasks[1:]:
        assert len(t.upstream_task_ids) == 1
