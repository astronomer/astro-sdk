import time
from datetime import datetime

from airflow import DAG
from astro.sql.table import Table

from astro import sql as aql
from astro.files import File
from astro.sql import drop_table

START_DATE = datetime(2000, 1, 1)


@aql.transform()
def get_top_five_animations(input_table: Table):  # skipcq: PYL-W0613
    return """
        SELECT title, rating
        FROM {{input_table}}
        WHERE genre1=='Animation'
        ORDER BY rating desc
        LIMIT 5;
    """


imdb_movies_name = "imdb_movies" + str(int(time.time()))

with DAG(
    "example_sqlite_load_transform",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:

    imdb_movies = aql.load_file(
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
        ),
        task_id="load_csv",
        output_table=Table(name=imdb_movies_name, conn_id="sqlite_default"),
    )

    top_five_animations = get_top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            name="top_animation",
            conn_id="sqlite_default",
        ),
    )
    # Note - Using persistent table just to showcase drop_table operator.
    # [START drop_table_example]
    truncate_results = drop_table(
        table=Table(name=imdb_movies_name, conn_id="sqlite_default")
    )
    # [END drop_table_example]
    truncate_results.set_upstream(top_five_animations)
    aql.cleanup()
