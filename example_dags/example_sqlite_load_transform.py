from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

START_DATE = datetime(2000, 1, 1)


# [START transform_example_1]  skipcq: PY-W0069
@aql.transform()
def top_five_animations(input_table: Table):
    return """
        SELECT Title, Rating
        FROM {{input_table}}
        WHERE Genre1=='Animation'
        ORDER BY Rating desc
        LIMIT 5;
    """


# [END transform_example_1]  skipcq: PY-W0069

with DAG(
    "example_sqlite_load_transform",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:

    imdb_movies = aql.load_file(
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv"
        ),
        task_id="load_csv",
        output_table=Table(name="imdb_movies", conn_id="sqlite_default"),
    )

    top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            name="top_animation",
            conn_id="sqlite_default",
        ),
    )
    aql.cleanup()
