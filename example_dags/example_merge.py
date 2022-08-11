import pathlib
from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.sql.table import Metadata, Table

CWD = pathlib.Path(__file__).parent

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_amazon_s3_postgres",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@aql.transform
def sample_create_table(input_table: Table):
    return "SELECT * FROM {{input_table}} LIMIT 10"


@aql.dataframe(columns_names_capitalization="original")
def my_df_func(input_df: DataFrame):
    print(input_df)


with dag:
    target_table = aql.load_file(
        input_file=File(str(pathlib.Path(CWD.parent.parent, "data/sample.csv"))),
        output_table=Table(
            conn_id="bigquery",
            metadata=Metadata(schema="first_table_schema"),
        ),
    )
    source_table = aql.load_file(
        input_file=File(str(pathlib.Path(CWD.parent.parent, "data/sample_part2.csv"))),
        output_table=Table(
            conn_id="bigquery",
            metadata=Metadata(schema="second_table_schema"),
        ),
    )
    # [START merge_example]
    aql.merge(
        target_table=target_table,
        source_table=source_table,
        target_conflict_columns=["id"],
        columns={"id": "id", "name": "name"},
        if_conflicts="update",
    )
    # [END merge_example]
