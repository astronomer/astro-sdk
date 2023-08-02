import tempfile
from datetime import datetime

import pandas as pd
from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.operators.export_to_file import ExportToFileOperator
from astro.table import Table


@aql.dataframe(columns_names_capitalization="original")
def top_movies_by_genre(input_df: pd.DataFrame):
    top_movies = []
    for _, genre_df in input_df.groupby("genre1"):
        genre_df.sort_values(by="rating", ascending=False)[["title", "rating", "genre1"]].head(5)
        top_movies.append(genre_df)
    return top_movies


with DAG(
    "calculate_popular_movies_by_genre",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    imdb_movies = aql.load_file(
        File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
        output_table=Table(conn_id="sqlite_default"),
    )
    movies_by_genre = top_movies_by_genre(input_df=imdb_movies)
    with tempfile.NamedTemporaryFile(suffix=".csv") as tmp_file:
        ExportToFileOperator.partial(
            if_exists="replace", output_file=File(path=tmp_file.name), task_id="export_movies"
        ).expand(input_data=movies_by_genre)
