import os
from datetime import datetime, timedelta

from astro.sql.dag import create_dag

default_args = {
    "retries": 1,
    "retry_delay": 0,
}

dir_path = os.path.dirname(os.path.realpath(__file__))

create_dag(
    dag_id="sql_file_dag",
    models_directory=dir_path + "/ingest_models",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)
