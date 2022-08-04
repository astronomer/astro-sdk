import pathlib
from datetime import datetime, timedelta

import sqlalchemy
from airflow.models import DAG

from astro import sql as aql
from astro.constants import FileType
from astro.files import File
from astro.sql.table import Metadata, Table

CWD = pathlib.Path(__file__).parent
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_load_file",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


with dag:
    # [load_file_example_1_start]
    my_homes_table = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
        output_table=Table(
            conn_id="postgres_conn",
        ),
    )
    # [load_file_example_1_end]

    # [load_file_example_2_start]
    dataframe = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
    )
    # [load_file_example_2_end]

    # [load_file_example_3_start]
    sample_table = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.ndjson"),
        output_table=Table(
            conn_id="postgres_conn",
        ),
        ndjson_normalize_sep="__",
    )
    # [load_file_example_3_end]

    # [load_file_example_4_start]
    new_table = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
        output_table=Table(
            conn_id="postgres_conn",
        ),
        if_exists="replace",
    )
    # [load_file_example_4_end]

    # [load_file_example_5_start]
    custom_schema_table = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
        output_table=Table(
            conn_id="postgres_conn",
            columns=[
                sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                sqlalchemy.Column(
                    "name", sqlalchemy.String(60), nullable=False, key="name"
                ),
            ],
        ),
    )
    # [load_file_example_5_end]

    # [load_file_example_6_start]
    dataframe = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
        columns_names_capitalization="upper",
    )
    # [load_file_example_6_end]

    # [load_file_example_7_start]
    aql.load_file(
        input_file=File("s3://tmp9/homes_main.csv", conn_id="aws_conn"),
        output_table=Table(conn_id="bigquery", metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [load_file_example_7_end]

    # [load_file_example_8_start]
    aql.load_file(
        input_file=File("s3://tmp9/homes_main.csv", conn_id="aws_conn"),
        output_table=Table(conn_id="bigquery", metadata=Metadata(schema="astro")),
        use_native_support=True,
        native_support_kwargs={
            "ignore_unknown_values": True,
            "allow_jagged_rows": True,
            "skip_leading_rows": "1",
        },
    )
    # [load_file_example_8_end]

    # [load_file_example_9_start]
    aql.load_file(
        input_file=File("s3://tmp9/homes_main.csv", conn_id="aws_conn"),
        output_table=Table(conn_id="bigquery", metadata=Metadata(schema="astro")),
        use_native_support=True,
        native_support_kwargs={
            "ignore_unknown_values": True,
            "allow_jagged_rows": True,
            "skip_leading_rows": "1",
        },
        enable_native_fallback=False,
    )
    # [load_file_example_9_end]

    # [load_file_example_10_start]
    my_homes_table = aql.load_file(
        input_file=File(
            path=str(CWD.parent) + "/../data/homes*", filetype=FileType.CSV
        ),
        output_table=Table(
            conn_id="postgres_conn",
        ),
    )
    # [load_file_example_10_end]
    aql.cleanup()
