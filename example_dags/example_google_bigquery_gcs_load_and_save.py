"""
This Example DAG:
 - Pulls a CSV file from Github and loads it into BigQuery.
 - Extracts the data from BigQuery and load into in-memory Pandas Dataframe
 - Finds the Top 5 movies based on Rating using pandas dataframe
 - And loads it into a Google Cloud Storage bucket in a CSV file

Pre-requisites:
 - Install dependencies for Astro Python SDK with Google, refer to README.md
 - Create an Airflow Connection to connect to Bigquery Table. Example:
    export AIRFLOW_CONN_BIGQUERY="bigquery://astronomer-dag-authoring"
 - You can either specify a service account key file and set `GOOGLE_APPLICATION_CREDENTIALS`
    with the file path to the service account.
"""
import os

import pandas as pd
from airflow.models.dag import DAG
from airflow.utils import timezone

import astro.sql as aql
from astro import dataframe
from astro.sql.tables import Metadata, Table

gcs_bucket = os.getenv("GCS_BUCKET", "gs://dag-authoring")

with DAG(
    dag_id="example_google_bigquery_gcs_load_and_save",
    schedule_interval=None,
    start_date=timezone.datetime(2022, 1, 1),
) as dag:
    t1 = aql.load_file(
        task_id="load_from_github_to_bq",
        path="https://raw.githubusercontent.com/astro-projects/astro/main/tests/data/imdb.csv",
        output_table=Table(
            name="imdb_movies", conn_id="bigquery", metadata=Metadata(schema="astro")
        ),
    )

    # Setting "identifiers_as_lower" to True will lowercase all column names
    @dataframe(identifiers_as_lower=False)
    def extract_top_5_movies(input_df: pd.DataFrame):
        print(f"Total Number of records: {len(input_df)}")
        top_5_movies = input_df.sort_values(by="Rating", ascending=False)[
            ["Title", "Rating", "Genre1"]
        ].head(5)
        print(f"Top 5 Movies: {top_5_movies}")
        return top_5_movies

    t2 = extract_top_5_movies(input_df=t1)

    aql.save_file(
        task_id="save_to_gcs",
        input_data=t2,
        output_file_path=f"{gcs_bucket}/{{ task_instance_key_str }}/top_5_movies.csv",
        output_file_format="csv",
        output_conn_id="gcp_conn",
        overwrite=True,
    )
