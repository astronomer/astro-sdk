from datetime import datetime

import pandas as pd
from airflow import DAG

from astro import sql as aql

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}
with DAG(
    "data_validation_check_column",
    schedule_interval=None,
    start_date=datetime(2000, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:
    # [START data_validation__check_column]
    df = pd.DataFrame(
        data={
            "name": ["Dwight Schrute", "Michael Scott", "Jim Halpert"],
            "age": [30, None, None],
            "city": [None, "LA", "California City"],
            "emp_id": [10, 1, 35],
        }
    )
    aql.check_column(
        dataset=df,
        column_mapping={
            "name": {"null_check": {"geq_to": 0, "leq_to": 1}},
            "city": {
                "null_check": {
                    "equal_to": 1,
                },
            },
            "age": {
                "null_check": {
                    "equal_to": 1,
                    "tolerance": 1,  # Tolerance is + and - the value provided. Acceptable values is 0 to 2.
                },
            },
        },
    )
    # [END data_validation__check_column]
