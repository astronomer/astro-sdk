import json
import os
import sys
import traceback
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import settings as benchmark_settings
from airflow import DAG
from astro import sql as aql
from astro.constants import DEFAULT_CHUNK_SIZE, FileType
from astro.files import File
from astro.sql.table import Metadata, Table
from run import export_profile_data_to_bq

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


def get_location(path):
    scheme = urlparse(path).scheme
    if scheme == "":
        return "local"
    return scheme


def get_traceback(exc) -> str:
    """
    Get traceback string from exception
    :param exc: Exception object
    """
    version = f"{sys.version_info.major}.{sys.version_info.minor}"
    # major = sys.version_info.major
    # minor = sys.version_info.minor

    if version in ["3.10"]:
        tb = traceback.format_exception(exc=exc, value=exc, tb=exc.__traceback__)
    elif version in ["3.9"]:
        tb = traceback.format_exception(value=exc, tb=exc.__traceback__)
    elif version in ["3.8"]:
        tb = traceback.format_exception(
            etype=type(exc), value=exc, tb=exc.__traceback__
        )
    return "".join(tb)


def create_dag(database_name, table_args, dataset, global_db_kwargs):
    """
    Create dag dynamically

    Override params from config
    example config:
    {
        "databases": [
            {
                "kwargs": {                            # global config bigquery db
                    "enable_native_fallback": false
                },
                "name": "biquery",
                "output_table": {
                    "conn_id": "biquery"
                }
            },{
                 "kwargs": {                          # global config for redshift db
                      "enable_native_fallback": false
                  },
                  "name": "redshift",
                  "output_table": {
                      "conn_id": "redshift"
                  }
            }],
          "datasets": [
                {
                    "conn_id": "aws",
                    "file_type": "parquet",
                    "name": "ten_kb",
                    "path": "s3://astro-sdk/benchmark/trimmed/covid_overview/covid_overview_10kb.csv",
                    "rows": 160,
                    "size": "10 KB",
                    "database_kwargs" : {
                         "biquery": {                           # local config for bigquery db
                             native_support_kwargs: {"skip_leading_rows": 1}
                         },
                         "redshift": {                          # local config for redshift db
                               native_support_kwargs:{
                                  "IGNOREHEADER": 1,
                                  "REGION": "us-west-2",
                                  "IAM_ROLE": "REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN",
                                }
                         }
                     }
                }]
    }

    Final config for load_file()
        1. Redshift
            {
                native_support_kwargs: {
                    "IGNOREHEADER": 1,
                    "REGION": "us-west-2",
                    "IAM_ROLE": REDSHIFT_NATIVE_LOAD_IAM_ROLE_ARN".
                },
                "enable_native_fallback": false
            }
        2.  Bigquery
            {
                native_support_kwargs: {
                    "skip_leading_rows": 1,
                },
                "enable_native_fallback": false
            }
    Note - If there is a config present in both local and global config.
    Local config will override the global one."""

    dataset_name = dataset["name"]
    dataset_path = dataset["path"]
    dataset_conn_id = dataset.get("conn_id")
    dataset_filetype = dataset.get("file_type")
    location = get_location(dataset_path)
    # dataset_rows = dataset["rows"]
    dataset_databases_kwargs = dataset.get("database_kwargs", {})
    local_db_kwargs = dataset_databases_kwargs.get(database_name, {})

    dag_name = (
        f"load_file_{dataset_name}_{dataset_filetype}_{location}_into_{database_name}"
    )

    def handle_failure(context):
        """
        Handle failures and publish data to bq
        :param context: Airflow taskinstance context
        """

        exc = context["exception"]

        exc_string = get_traceback(exc)
        profile = {
            "database": database_name,
            "filetype": dataset_filetype,
            "path": dataset_path,
            "dataset": dataset_name,
            "error": "True",
            "error_context": exc_string,
        }

        if benchmark_settings.publish_benchmarks:
            export_profile_data_to_bq(profile)

    with DAG(dag_name, schedule_interval=None, start_date=START_DATE) as dag:
        chunk_size = int(os.getenv("ASTRO_CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
        table_metadata = table_args.pop("metadata", {})
        if table_metadata:
            table = Table(metadata=Metadata(**table_metadata), **table_args)
        else:
            table = Table(**table_args)

        params = {
            "input_file": File(
                path=dataset_path,
                conn_id=dataset_conn_id,
                filetype=FileType(dataset_filetype),
            ),
            "task_id": "load",
            "output_table": table,
            "chunk_size": chunk_size,
        }
        if local_db_kwargs.get("skip"):
            local_db_kwargs.pop("skip")
        params.update(global_db_kwargs)
        params.update(local_db_kwargs)
        my_table = aql.load_file(**params, on_failure_callback=handle_failure)
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
    table_args = database["output_table"]
    global_db_kwargs = database["kwargs"]
    for dataset in config["datasets"]:
        table_args_copy = table_args.copy()

        dag = create_dag(database_name, table_args_copy, dataset, global_db_kwargs)
        globals()[dag.dag_id] = dag
