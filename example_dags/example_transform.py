from datetime import datetime

import pandas as pd
from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

START_DATE = datetime(2000, 1, 1)
LAST_ONE_DF = pd.DataFrame(data={"Title": ["Random movie"], "Rating": [121]})


# [START transform_example_1]  skipcq: PY-W0069
@aql.transform()
def top_five_animations(input_table: Table):
    return """
        SELECT *
        FROM {{input_table}}
        WHERE Genre1=='Animation'
        ORDER BY Rating desc
        LIMIT 5;
    """


# [END transform_example_1]  skipcq: PY-W0069


# [START transform_example_2]  skipcq: PY-W0069
@aql.transform()
def last_five_animations(input_table: Table):
    return """
        SELECT *
        FROM {{input_table}}
        WHERE Genre1=='Animation'
        ORDER BY Rating asc
        LIMIT 5;
    """


# [END transform_example_2]  skipcq: PY-W0069


# [START transform_example_3]  skipcq: PY-W0069
@aql.transform
def union_top_and_last(first_table: Table, second_table: Table):
    """Union `first_table` and `second_table` tables to create a simple dataset."""
    return """
            SELECT Title, Rating from {{first_table}}
            UNION
            SELECT Title, Rating from {{second_table}};
            """


# [END transform_example_3]  skipcq: PY-W0069


# [START transform_example_4]  skipcq: PY-W0069
@aql.transform
def union_table_and_dataframe(input_table: Table, input_dataframe: pd.DataFrame):
    """Union `union_table` table and `input_dataframe` dataframe to create a simple dataset."""
    return """
            SELECT Title, Rating from {{input_table}}
            UNION
            SELECT Title, Rating from {{input_dataframe}};
            """


# [END transform_example_4]  skipcq: PY-W0069


with DAG(
    "example_transform",
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

    top_five = top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            name="top_animation",
            conn_id="sqlite_default",
        ),
    )

    last_five = last_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            name="last_animation",
            conn_id="sqlite_default",
        ),
    )

    union_table = union_top_and_last(top_five, last_five)

    union_table_and_dataframe(union_table, LAST_ONE_DF)

    aql.cleanup()
