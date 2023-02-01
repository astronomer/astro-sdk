from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.table import Table

with DAG(
    "data_validation_check_table",
    schedule_interval=None,
    start_date=datetime(2000, 1, 1),
    catchup=False,
) as dag:
    # [START data_validation__check_table]
    imdb_movies = aql.load_file(
        File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/python-sdk/tests/data/homes.csv"),
        output_table=Table(conn_id="sqlite_default"),
    )
    aql.check_table(
        dataset=imdb_movies,
        checks={
            "sell_list": {"check_statement": "sell <= list"},
            "row_count": {"check_statement": "Count(*) = 47"},
        },
    )
    # [END data_validation__check_table]
