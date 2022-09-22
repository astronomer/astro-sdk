from datetime import datetime

import pandas as pd
from airflow import DAG
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

START_DATE = datetime(2000, 1, 1)
LAST_ONE_DF = pd.DataFrame(data={"title": ["Random movie"], "rating": [121]})


# [START transform_example_1]
@aql.transform()
def top_five_animations(input_table: Table):  # skipcq: PYL-W0613
    return """
        SELECT *
        FROM {{input_table}}
        WHERE genre1=='Animation'
        ORDER BY rating desc
        LIMIT 5;
    """


# [END transform_example_1]


# [START transform_example_2]
@aql.transform()
def last_five_animations(input_table: Table):  # skipcq: PYL-W0613
    return """
        SELECT *
        FROM {{input_table}}
        WHERE genre1=='Animation'
        ORDER BY rating asc
        LIMIT 5;
    """


# [END transform_example_2]


# [START transform_example_3]
@aql.transform
def union_top_and_last(first_table: Table, second_table: Table):  # skipcq: PYL-W0613
    """Union `first_table` and `second_table` tables to create a simple dataset."""
    return """
            SELECT title, rating from {{first_table}}
            UNION
            SELECT title, rating from {{second_table}};
            """


# [END transform_example_3]


# [START transform_example_4]
@aql.transform
def union_table_and_dataframe(
    input_table: Table, input_dataframe: pd.DataFrame  # skipcq: PYL-W0613
):
    """Union `union_table` table and `input_dataframe` dataframe to create a simple dataset."""
    return """
            SELECT title, rating from {{input_table}}
            UNION
            SELECT title, rating from {{input_dataframe}};
            """


# [END transform_example_4]


with DAG(
    "example_transform",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:

    imdb_movies = aql.load_file(
        input_file=File(path="s3://astro-sdk/imdb_v2.csv"),
        task_id="load_csv",
        output_table=Table(conn_id="sqlite_default"),
    )

    top_five = top_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            conn_id="sqlite_default",
        ),
    )

    last_five = last_five_animations(
        input_table=imdb_movies,
        output_table=Table(
            conn_id="sqlite_default",
        ),
    )

    union_table = union_top_and_last(top_five, last_five)

    union_table_and_dataframe(union_table, LAST_ONE_DF)

    aql.cleanup()
