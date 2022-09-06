from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

input_file = File(path="gs://dag-authoring/movies/imdb_v2.csv")
imdb_movies_table = Table(name="imdb_movies", conn_id="postgres_default")
top_animations_table = Table(name="top_animation", conn_id="postgres_default")
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


with DAG(
    dag_id="load_file",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as l_dag:
    imdb_movies = aql.load_file(
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
        ),
        task_id="load_csv",
        output_table=imdb_movies_table,
    )

with DAG(
    dag_id="transform_top_animations",
    schedule=[imdb_movies_table],
    start_date=START_DATE,
    catchup=False,
) as t_dag:
    top_five_animations = get_top_five_animations(
        input_table=imdb_movies_table,
        output_table=top_animations_table,
    )
