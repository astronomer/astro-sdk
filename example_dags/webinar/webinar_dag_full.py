"""
reddit_airflow_pipeline
"""
from datetime import datetime

import pandas as pd
import praw
from airflow.decorators import dag
from airflow.models import Variable

from astro import sql as aql
from astro.sql.table import Table

reddit = praw.Reddit(
    client_id=Variable.get("REDDIT_CLIENT_ID"),
    client_secret=Variable.get("REDDIT_CLIENT_SECRET"),
    user_agent="Learn About AIrflow",
)

subreddits = [
    "dataengineering",
    "data",
    "datascience",
    "learnpython",
    "ETL",
    "dataengineeringjobs",
    "BigDataJobs",
    "BigDataETL",
    "dataanalysis",
    "DataScienceJobs",
    "MachineLearning",
]


@aql.dataframe()
def find_airflow_posts_func():
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


@dag(
    schedule_interval="@daily", start_date=datetime(2022, 7, 27), catchup=False, tags=[]
)
def reddit_airflow_pipeline():
    find_airflow_posts = find_airflow_posts_func(
        output_table=Table(conn_id="snowflake_conn")
    )
    aql.merge(
        target_table=Table(name="REDDIT_POSTS", conn_id="snowflake_conn"),
        source_table=find_airflow_posts,
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
        target_conflict_columns=["title", "id"],
        if_conflicts="update",
    )
    aql.cleanup()


dag_obj = reddit_airflow_pipeline()
