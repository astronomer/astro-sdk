import logging
import time
from datetime import datetime

import pandas
from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.table import Table

START_DATE = datetime(2000, 1, 1)
log = logging.getLogger(__name__)


@aql.transform()
def get_top_five_animations(input_table: Table):  # skipcq: PYL-W0613
    return """
        SELECT title, rating
        FROM {{input_table}}
        WHERE genre1=='Animation'
        ORDER BY rating desc
        LIMIT 5;
    """


@aql.dataframe
def print_top_animation(df: pandas.DataFrame):
    log.info("top movie: %s", str(df.iloc[0]))


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
        output_table=Table(name=imdb_movies_name, conn_id="my_test_sqlite"),
    )

    top_five_animations = get_top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            name="top_animation",
            conn_id="my_test_sqlite",
        ),
    )
    print_top_animation(top_five_animations)
    aql.cleanup()
