import argparse
import inspect
import json
import os
import sys
from urllib.parse import urlparse

import airflow
import pandas as pd
import psutil
import settings as benchmark_settings
from airflow.executors.debug_executor import DebugExecutor
from airflow.models import TaskInstance
from airflow.utils import timezone
from airflow.utils.session import provide_session
from astro.databases import create_database
from astro.sql.table import Metadata, Table


def get_disk_usage():
    path = "/"
    disk_usage = psutil.disk_usage(path)
    return disk_usage.used


def subtract(after_dict, before_dict):
    return {key: after_dict[key] - before_dict.get(key, 0) for key in after_dict}


def export_profile_data_to_bq(profile_data: dict, conn_id: str = "bigquery"):
    """Save profile data to bigquery table

    :param profile_data: profiling data collected
    :param conn_id: Airflow's connection id to be used to publish the profiling data
    """
    try:
        db = create_database(conn_id)
        if profile_data.get("io_counters", False):
            del profile_data["io_counters"]
        df = pd.json_normalize(profile_data, sep="_")
        table = Table(
            name=benchmark_settings.publish_benchmarks_table,
            metadata=Metadata(schema=benchmark_settings.publish_benchmarks_schema),
        )
        db.load_pandas_dataframe_to_table(df, table, if_exists="append")
    except Exception as e:
        print("=========================================")
        print(e)


@provide_session
def get_load_task_duration(dag, session=None):
    ti: TaskInstance = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.dag_id == dag.dag_id,
            TaskInstance.task_id == "load",
            TaskInstance.execution_date == dag.latest_execution_date,
        )
        .first()
    )
    return ti.duration


def get_location(path):
    scheme = urlparse(path).scheme
    if scheme == "":
        return "local"
    return scheme


def profile(func, *args, **kwargs):  # noqa: C901
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        # metrics before
        memory_full_info_before = process.memory_full_info()._asdict()
        cpu_time_before = process.cpu_times()._asdict()
        disk_usage_before = get_disk_usage()
        if sys.platform == "linux":
            io_counters_before = process.io_counters()._asdict()

        skip = kwargs.pop("skip", False)

        profile = {
            "database": kwargs.get("database"),
            "filetype": kwargs.get("filetype"),
            "path": kwargs.get("path"),
            "revision": kwargs.get("revision"),
            "dataset": kwargs.get("dataset"),
            "error": "False",
            "error_context": "Skipped",
        }

        if not skip:
            # run decorated function
            dag = func(*args, **kwargs)

            # metrics after
            memory_full_info_after = process.memory_full_info()._asdict()
            cpu_time_after = process.cpu_times()._asdict()
            disk_usage_after = get_disk_usage()
            if sys.platform == "linux":
                io_counters_after = process.io_counters()._asdict()

            profile = {
                "duration": get_load_task_duration(dag=dag),
                "memory_full_info": subtract(
                    memory_full_info_after, memory_full_info_before
                ),
                "cpu_time": subtract(cpu_time_after, cpu_time_before),
                "disk_usage": disk_usage_after - disk_usage_before,
            }
            if sys.platform == "linux":
                profile["io_counters"] = (
                    subtract(io_counters_after, io_counters_before),
                )

            profile = {**profile, **kwargs, "error": "False", "error_context": None}

        print(json.dumps(profile, default=str))
        if benchmark_settings.publish_benchmarks:
            export_profile_data_to_bq(profile)

    if inspect.isfunction(func):
        return wrapper
    elif inspect.ismethod(func):
        return wrapper(*args, **kwargs)


@profile
def run_dag(dag_id, execution_date, **kwargs):
    dagbag = airflow.models.DagBag()
    dag = dagbag.get_dag(dag_id)
    dag.clear(start_date=execution_date, end_date=execution_date, dag_run_state=False)

    dag.run(
        executor=DebugExecutor(),
        start_date=execution_date,
        end_date=execution_date,
        # Always run the DAG at least once even if no logical runs are
        # available. This does not make a lot of sense, but Airflow has
        # been doing this prior to 2.2 so we keep compatibility.
        run_at_least_once=True,
    )
    return dag


def build_dag_id(dataset, database, filetype, location):
    return f"load_file_{dataset}_{filetype}_{location}_into_{database}"


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trigger benchmark DAG")
    parser.add_argument(
        "--database",
        type=str,
        help="Database {snowflake, bigquery, postgres}",
        required=True,
    )
    parser.add_argument(
        "--dataset",
        type=str,
        help="Dataset {few_kb, many_kb, few_mb, many_mb}",
        required=True,
    )
    parser.add_argument(
        "--filetype",
        type=str,
        help="filetype {csv, ndjson, parquet}",
        required=True,
    )
    parser.add_argument(
        "--path",
        type=str,
        help="dataset path",
        required=True,
    )
    parser.add_argument(
        "--skip",
        type=str,
        help="Skip this dataset from benchmarking. "
        "This is useful because we still record in db that this dataset/db combo is skipped",
        required=True,
    )
    parser.add_argument(
        "--revision",
        type=str,
        help="Git commit hash, to relate the results to a version",
    )
    parser.add_argument(
        "--chunk-size",
        "-c",
        type=int,
        help="Chunk size used for loading from file to database. Default: [1,000,000]",
    )
    args = parser.parse_args()
    location = get_location(args.path)
    dag_id = build_dag_id(args.dataset, args.database, args.filetype, location)
    run_dag(
        dag_id=dag_id,
        execution_date=timezone.utcnow(),
        revision=args.revision,
        chunk_size=args.chunk_size,
        database=args.database,
        filetype=args.filetype,
        dataset=args.dataset,
        path=args.path,
        skip=args.skip,
    )
