from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.sql.table import Table

START_DATE = datetime(2000, 1, 1)


@aql.transform
def top_five_scify_movies(input_table: Table):
    return """
        SELECT COUNT(*)
        FROM {{input_table}}
    """


with DAG(
    "example_sqlite_load_transform", schedule_interval=None, start_date=START_DATE
) as dag:

    imdb_movies = aql.load_file(
        path="https://raw.githubusercontent.com/astro-projects/astro/readme/tests/data/imdb.csv",
        task_id="load_csv",
        output_table=Table(
            table_name="imdb_movies", database="sqlite", conn_id="sqlite_default"
        ),
    )

    top_five_scify_movies(
        input_table=imdb_movies,
        output_table=Table(
            table_name="top_scify", database="sqlite", conn_id="sqlite_default"
        ),
    )
