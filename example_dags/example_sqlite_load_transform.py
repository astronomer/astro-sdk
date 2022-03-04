from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.sql.table import Table

START_DATE = datetime(2000, 1, 1)


@aql.transform()
def top_five_animations(input_table: Table):
    return """
        SELECT Title, Rating
        FROM {{input_table}}
        WHERE Genre1=='Animation'
        ORDER BY Rating desc
        LIMIT 5;
    """


with DAG(
    "example_sqlite_load_transform",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:

    imdb_movies = aql.load_file(
        path="https://raw.githubusercontent.com/astro-projects/astro/main/tests/data/imdb.csv",
        task_id="load_csv",
        output_table=Table(
            table_name="imdb_movies", database="sqlite", conn_id="sqlite_default"
        ),
    )

    top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            table_name="top_animation", database="sqlite", conn_id="sqlite_default"
        ),
    )
