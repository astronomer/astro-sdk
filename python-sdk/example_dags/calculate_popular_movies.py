from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table


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
    "calculate_popular_movies",
    schedule_interval=None,
    start_date=datetime(2000, 1, 1),
    catchup=False,
) as dag:
    imdb_movies = aql.load_file(
        File(
            "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv"
        ),
        output_table=Table(conn_id="sqlite_default"),
    )
    top_five_animations(
        input_table=imdb_movies,
        output_table=Table(name="top_animation"),
    )
    aql.cleanup()
