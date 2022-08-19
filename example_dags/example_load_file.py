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
    # [START load_file_example_1]
    my_homes_table = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
        output_table=Table(
            conn_id="postgres_conn",
        ),
    )
    # [END load_file_example_1]

    # [START load_file_example_2]
    dataframe = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
    )
    # [END load_file_example_2]

    # [START load_file_example_3]
    sample_table = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.ndjson"),
        output_table=Table(
            conn_id="postgres_conn",
        ),
        ndjson_normalize_sep="__",
    )
    # [END load_file_example_3]

    # [START load_file_example_4]
    new_table = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
        output_table=Table(
            conn_id="postgres_conn",
        ),
        if_exists="replace",
    )
    # [END load_file_example_4]

    # [START load_file_example_5]
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
    # [END load_file_example_5]

    # [START load_file_example_6]
    dataframe = aql.load_file(
        input_file=File(path="s3://astro-sdk/sample.csv"),
        columns_names_capitalization="upper",
    )
    # [END load_file_example_6]

    # [START load_file_example_7]
    aql.load_file(
        input_file=File("s3://tmp9/homes_main.csv", conn_id="aws_conn"),
        output_table=Table(conn_id="bigquery", metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_example_7]

    # [START load_file_example_8]
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
    # [END load_file_example_8]

    # [START load_file_example_9]
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
    # [END load_file_example_9]

    # [START load_file_example_10]
    my_homes_table = aql.load_file(
        input_file=File(
            path=str(CWD.parent) + "/tests/data/homes*", filetype=FileType.CSV
        ),
        output_table=Table(
            conn_id="postgres_conn",
        ),
    )
    # [END load_file_example_10]

    # [START load_file_example_11]
    aql.load_file(
        input_file=File(
            "s3://astro-sdk/sample_pattern", conn_id="aws_conn", filetype=FileType.CSV
        ),
        output_table=Table(conn_id="bigquery", metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_example_11]

    # [START load_file_example_12]
    aql.load_file(
        input_file=File(
            "gs://astro-sdk/workspace/sample_pattern",
            conn_id="bigquery",
            filetype=FileType.CSV,
        ),
        output_table=Table(conn_id="bigquery", metadata=Metadata(schema="astro")),
        use_native_support=False,
    )
    # [END load_file_example_12]

    aql.cleanup()
