import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from astro.constants import Database
from astro.sql.table import create_unique_table_name
from astro.utils.delete import delete_dataframe_rows_from_table

table_name = create_unique_table_name()

DEFAULT_SQLITE_CONN_ID = "sqlite_default"


def create_table(database, hook, table):
    hook.run(f"DROP TABLE IF EXISTS {table.qualified_name}")
    if database == Database.BIGQUERY.value:
        hook.run(f"CREATE TABLE {table.qualified_name} (ID int, Name string);")
    else:
        hook.run(f"CREATE TABLE {table.qualified_name} (ID int, Name varchar(255));")
    hook.run(f"INSERT INTO {table_name} (ID, Name) VALUES (1, 'Janis Joplin');")
    hook.run(f"INSERT INTO {table_name} (ID, Name) VALUES (2, 'Jimi Hendrix');")


@pytest.mark.integration
@pytest.mark.parametrize("sql_server", ["sqlite"], indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [{"is_temp": False, "param": {"name": table_name}}],
    ids=["named_table"],
    indirect=True,
)
def test_delete_dataframe_rows_from_table(test_table, sql_server):
    database, hook = sql_server
    original_table = test_table
    create_table(database, hook, original_table)
    dataframe = pd.DataFrame([{"id": 2, "name": "Jimi Hendrix"}])
    delete_dataframe_rows_from_table(dataframe, original_table, hook)
    df = hook.get_pandas_df(f"SELECT * FROM {original_table.name}")
    df = df.rename(columns=str.lower)

    assert len(df) == 1
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "Janis Joplin"},
        ]
    )
    assert_frame_equal(df, expected)
