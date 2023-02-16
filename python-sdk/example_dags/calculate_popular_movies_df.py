from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task

from astro import sql as aql
from astro.files import File
from astro.table import Table


@aql.dataframe(columns_names_capitalization="original")
def top_five_animations(input_df: pd.DataFrame):
    print(f"Total Number of records: {len(input_df)}")
    top_5_movies = input_df.sort_values(by="rating", ascending=False)[["title", "rating", "genre1"]].head(5)
    print(f"Top 5 Movies: {top_5_movies}")
    return top_5_movies


with DAG(
    "calculate_top_2_movies_using_dataframe",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    imdb_movies = aql.load_file(
        File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
        output_table=Table(conn_id="sqlite_default"),
    )
    top_five_movies_task = top_five_animations(input_df=imdb_movies)

    # The calculation of top 2 movies is purposely done in a separate task using @task decorator
    # so that we can test that dataframe is correctly stored in XCom and passed to the following task
    @task
    def top_two_movies(input_df: pd.DataFrame):
        top_2 = input_df.head(2)
        print(f"Top 2 movies: {top_2}")

    top_two_movies(top_five_movies_task)
