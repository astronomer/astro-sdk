import json
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from astro.constants import DEFAULT_CHUNK_SIZE, FileType
from astro.sql.table import Metadata, Table

from astro import sql as aql
from astro.files import File

START_DATE = datetime(2000, 1, 1)


def load_config():
    config_path = Path(Path(__file__).parent.parent, "config.json").resolve()
    with open(config_path) as fp:
        return json.load(fp)


@aql.transform
def count_rows(input_table: Table):
    return """
        SELECT COUNT(*)
        FROM {input_table}
    """


@aql.transform
def count_columns(input_table: Table):
    return """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name={input_table};
    """


def create_dag(database_name, table_args, dataset):
    dataset_name = dataset["name"]
    dataset_path = dataset["path"]
    dataset_conn_id = dataset.get("conn_id")
    dataset_filetype = dataset.get("file_type")
    # dataset_rows = dataset["rows"]

    dag_name = f"load_file_{dataset_name}_into_{database_name}"

    with DAG(dag_name, schedule_interval=None, start_date=START_DATE) as dag:
        chunk_size = int(os.getenv("ASTRO_CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
        table_metadata = table_args.pop("metadata", {})
        if table_metadata:
            table = Table(metadata=Metadata(**table_metadata), **table_args)
        else:
            table = Table(**table_args)

        my_table = aql.load_file(  # noqa: F841
            input_file=File(
                path=dataset_path,
                conn_id=dataset_conn_id,
                filetype=FileType(dataset_filetype),
            ),
            task_id="load",
            output_table=table,
            chunk_size=chunk_size,
        )
        aql.cleanup([my_table])

        # Todo: Check is broken so the following code is commented out, uncomment when fixed
        # aggregate_check(
        #    table_xcom,
        #    check="SELECT COUNT(*) FROM {table}",
        #    equal_to=dataset_rows
        # )
        # table_xcom.set_downstream(aggregate_check)

        return dag


config = load_config()
for database in config["databases"]:
    database_name = database["name"]
    table_args = database["params"]
    for dataset in config["datasets"]:
        table_args_copy = table_args.copy()

        dag = create_dag(database_name, table_args_copy, dataset)
        globals()[dag.dag_id] = dag
