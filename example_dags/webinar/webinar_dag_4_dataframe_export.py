import os
import time
from datetime import datetime, timedelta

import pandas
import pandas as pd

# Uses data from https://www.kaggle.com/c/shelter-animal-outcomes
from airflow.models import DAG

from astro import constants
from astro import sql as aql
from astro.files import File
from astro.sql.table import Metadata, Table

dag = DAG(
    dag_id="my_webinar_dag",
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    default_args={
        "email_on_failure": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
)


def run_against_model(df: pd.DataFrame):
    return df


@aql.dataframe()
def run_against_model(df: pd.DataFrame):
    new_df = run_against_model(df)
    return new_df


import praw

reddit = praw.Reddit(
    client_id=Variable.get("REDDIT_CLIENT_ID"),
    client_secret=Variable.get("REDDIT_CLIENT_SECRET"),
    user_agent="Learn About AIrflow",
)


@aql.dataframe()
def find_airflow_posts_func(date):
    keyword = "airflow"
    posts = []

    for sub in subreddits:
        for post in reddit.subreddit(sub).new(limit=100):
            if keyword in post.title.lower() or keyword in post.selftext.lower():
                posts.append(
                    [
                        post.title,
                        post.score,
                        post.id,
                        post.url,
                        post.num_comments,
                        post.selftext,
                        post.created,
                        post.subreddit.display_name,
                    ]
                )

    posts = pd.DataFrame(
        posts,
        columns=[
            "title",
            "score",
            "id",
            "url",
            "num_comments",
            "body",
            "created",
            "subreddit",
        ],
    )

    return posts


from library.path import find_airflow_posts_func

from astro import sql as aql
from astro.files import File


@aql.transform
def filter_duplicate_posts(post: Table):
    return "SELECT DISTINCT * FROM {{post}}"


with dag:
    reddit_posts_df = find_airflow_posts_func(date="{{ execution_date }}")

from airflow.decorators import task


@task
def do_df_thing(df: pandas.DataFrame):
    # Your dataframe transformation here
    return df


s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

with dag:
    reddit_posts_2019 = aql.load_file(
        input_file=File(
            path=f"{s3_bucket}/REDDIT_DATA_2019.csv", conn_id="my_s3_conn_id"
        ),
    )
    reddit_posts_2020 = aql.load_file(
        input_file=File(
            path=f"{s3_bucket}/REDDIT_DATA_2020.csv", conn_id="my_s3_conn_id"
        ),
    )
    do_df_thing(reddit_posts_2019)

s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

from library.path import find_airflow_posts_func

from astro import constants
from astro import sql as aql
from astro.files import File

with dag:
    reddit_posts = find_airflow_posts_func(date="{{ execution_date }}")
    aql.export_file(
        input_data=reddit_posts,
        output_file=File(
            path="s3://path/to/reddit_output_{{ execution_date }}.csv",
            filetype=constants.FileType.CSV,
        ),
    )
