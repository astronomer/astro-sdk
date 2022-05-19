import os
from datetime import datetime

from airflow.models import DAG, Param

from astro.sql import load_file, render
from astro.sql.table import Metadata, Table

SNOWFLAKE_CONN_ID = "snowflake_conn"
dir_path = os.path.dirname(os.path.realpath(__file__))

FILE_PATH = dir_path + "/data/"

"""
This DAG highlights using the render function to execute SQL queries.
Here the render function results in three sequential tasks that run
queries in the /include/sql directory. The queries, combine, clean, and
aggregate data in Snowflake.

To use the DAG, you must have two tables called "Homes" and "Homes2" in
your database. The `load_file` tasks in this DAG load the data from homes csv's in the `data/` directory.
You must also update the frontmatter in the queries to your own database
connection info.
"""

dag = DAG(
    dag_id="example_snowflake_render",
    start_date=datetime(2022, 2, 1),
    schedule_interval="@daily",
    catchup=False,
    params={
        "min_rooms": Param(0, type="integer"),
        "max_rooms": Param(50, type="integer"),
    },
    template_searchpath=os.path.dirname(os.path.realpath(__file__)),
)

with dag:
    homes_data1 = load_file(
        path=FILE_PATH + "homes.csv",
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            ),
        ),
    )

    homes_data2 = load_file(
        path=FILE_PATH + "homes2.csv",
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            ),
        ),
    )

    homes_models = render(
        "demo_parse_directory/homes_example/",
        homes=homes_data1,
        homes2=homes_data2,
    )
