from datetime import datetime
from pathlib import Path

from airflow import DAG

from astro import sql as aql
from astro.files import File
from astro.table import Table

START_DATE = datetime(2000, 1, 1)


with DAG(
    "example_transform_file",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:
    imdb_movies = aql.load_file(
        input_file=File("https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
        task_id="load_csv",
        output_table=Table(conn_id="sqlite_default"),
    )
    target_table = Table(name="test_is_{{ ds_nodash }}", conn_id="sqlite_default")

    # [START transform_file_example_1]
    table_from_query = aql.transform_file(
        file_path=f"{Path(__file__).parent.as_posix()}/demo_parse_directory/transform.sql",
        parameters={"input_table": imdb_movies},
        op_kwargs={"output_table": target_table},
    )
    # [END transform_file_example_1]
