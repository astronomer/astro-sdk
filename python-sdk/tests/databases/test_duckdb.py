from astro.databases.duckdb import DuckdbDatabase


def test_get_merge_initialization_query():
    parameters = ("col_1", "col_2")

    sql = DuckdbDatabase.get_merge_initialization_query(parameters)
    assert sql == "CREATE UNIQUE INDEX merge_index ON {{table}}(col_1,col_2)"
