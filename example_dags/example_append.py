import pathlib
from datetime import datetime, timedelta

from airflow.models import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

CWD = pathlib.Path(__file__).parent

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_append",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)

DATA_DIR = str(CWD) + "/data/"

with dag:
    load_main = aql.load_file(
        input_file=File(path=DATA_DIR + "homes.csv"),
        output_table=Table(conn_id="postgres_conn"),
    )
    load_append = aql.load_file(
        input_file=File(path=DATA_DIR + "/homes2.csv"),
        output_table=Table(conn_id="postgres_conn"),
    )
    # [START append_example]
    aql.append(
        target_table=load_main,
        source_table=load_append,
    )
    # [END append_example]

    # [START append_example_col_dict]
    aql.append(
        target_table=load_main, source_table=load_append, columns={"beds": "baths"}
    )
    # [END append_example_col_dict]

    aql.cleanup()
