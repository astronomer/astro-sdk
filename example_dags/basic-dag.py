from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

from astro import sql as aql
from astro.dataframe import dataframe as df
from astro.sql.table import Table, TempTable

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="astro_test_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@aql.transform
def films_in_drama_category():
    return "SELECT film_id FROM film INNER JOIN film_category ON (film.film_id=film_category.film_id) WHERE film_category.category_id=6;"


@aql.transform
def actors_c_lastname():
    return "SELECT * FROM actor WHERE last_name LIKE 'C%';"


@aql.transform
def actor_with_c_name_in_drama_films(actor: Table, drama_films: Table):
    return "SELECT DISTINCT first_name, last_name FROM {actor} INNER JOIN {drama_films} ON (actor.actor_id=tmp_astro.drama_actors.actor_id);"


@df
def print_result(input_df: DataFrame):
    print(input_df)


with dag:
    my_homes_table = aql.load_file(
        path="s3://tmp9/homes.csv",
        output_table=TempTable(
            database="pagila",
            conn_id="postgres_conn",
        ),
    )
    all_actors_c_lastname = actors_c_lastname(
        conn_id="postgres_conn", database="pagila"
    )
    drama_films = films_in_drama_category(conn_id="postgres_conn", database="pagila")
    actors_of_interest = actor_with_c_name_in_drama_films(
        actor=all_actors_c_lastname, drama_films=drama_films
    )
    print_result(actors_of_interest)
