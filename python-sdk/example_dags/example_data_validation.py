import pathlib
from datetime import datetime, timedelta

from airflow.models import DAG

from astro import sql as aql
from astro.files import File
from astro.table import Table

CWD = pathlib.Path(__file__).parent

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_data_validation",
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
    aql.cleanup()
