# NOTE: This is an auto-generated file. Please do not edit this file manually.
from airflow import DAG
from airflow.utils import timezone
from astro import sql as aql
from astro.sql.table import Metadata, Table

with DAG(
    dag_id="advanced",
    start_date=timezone.parse("2022-10-04 11:47:42.393451"),
    schedule_interval=None,
) as dag:
    source__imdb_movies = aql.transform_sql(
        sql="_target/advanced/source/imdb_movies.sql",
        parameters={
        },
        conn_id="postgres_conn",
        op_kwargs={
            "output_table": Table(
                name="imdb_movies",
            ),
        },
        task_id="source__imdb_movies",
    )
    source__last_five_animations = aql.transform_sql(
        sql="_target/advanced/source/last_five_animations.sql",
        parameters={
            "source__imdb_movies": source__imdb_movies,
        },
        op_kwargs={
            "output_table": Table(
                name="last_five_animations",
                metadata=Metadata(
                    schema="public",
                ),
            ),
        },
        task_id="source__last_five_animations",
    )
    source__top_five_animations = aql.transform_sql(
        sql="_target/advanced/source/top_five_animations.sql",
        parameters={
            "source__imdb_movies": source__imdb_movies,
        },
        op_kwargs={
            "output_table": Table(
                name="top_five_animations",
            ),
        },
        task_id="source__top_five_animations",
    )
    mart__union_top_and_last = aql.transform_sql(
        sql="_target/advanced/mart/union_top_and_last.sql",
        parameters={
            "source__last_five_animations": source__last_five_animations,
            "source__top_five_animations": source__top_five_animations,
        },
        op_kwargs={
            "output_table": Table(
                name="union_top_and_last",
                metadata=Metadata(
                    database="postgres",
                ),
            ),
        },
        task_id="mart__union_top_and_last",
    )
