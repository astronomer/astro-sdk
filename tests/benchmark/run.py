import argparse
import inspect
import json
import os
import sys
import time

import airflow
import pandas as pd
import psutil
from airflow.executors.debug_executor import DebugExecutor
from airflow.utils import timezone

from astro.databases import create_database


def elapsed_since(start):
    return time.time() - start


def get_disk_usage():
    path = "/"
    disk_usage = psutil.disk_usage(path)
    return disk_usage.used


def subtract(after_dict, before_dict):
    return {key: after_dict[key] - before_dict.get(key, 0) for key in after_dict}


def prep_insert_statement(
    df: pd.DataFrame, table_name="load_files_to_database", schema="Benchmark"
) -> str:
    """Prepare the insert statement for data received

    :param df: dataframe to be inserted to db. Note - We assume there is only one row in this dataframe
    :param table_name: pre-existing database table name.
     Note - we assume this table exists and is created based on the profiling data that needs to be saved.
     table name: load_files_to_database
     table project: astronomer-dag-authoring
     table schema:
        duration, FLOAT
        disk_usage, FLOAT
        dag_id, STRING
        execution_date, DATETIME
        revision, STRING
        chunk_size, INTEGER
        memory_full_info__rss, FLOAT
        memory_full_info__vms, FLOAT
        memory_full_info__pfaults, FLOAT
        memory_full_info__pageins, FLOAT
        memory_full_info__uss, FLOAT
        cpu_time__user, FLOAT
        cpu_time__system, FLOAT
        cpu_time__children_user, FLOAT
        cpu_time__children_system, FLOAT

    :param schema: schema of pre-existing table
    """
    wrapper_for_cols = {
        "dag_id": ["'", "'"],
        "execution_date": ["DATETIME(TIMESTAMP '", "')"],
        "revision": ["'", "'"],
    }
    cols = list(df.columns)
    vals = []
    for col in cols:
        if col in wrapper_for_cols:
            wrapper = wrapper_for_cols[col]
            vals.append(wrapper[0] + str(df[col][0]) + wrapper[1])
        else:
            vals.append(str(df[col][0]))

    return (
        f"INSERT {schema}.{table_name} ({', '.join(cols)}) VALUES ({', '.join(vals)})"
    )


def prep_profile_data(profile_data: dict) -> pd.DataFrame:
    """Normalize profile data as the data is not suitable for publishing in db table

    :param profile_data: profiling data collected
    """
    normalize_delimiter = "__"
    normalize_config = {
        "meta_prefix": normalize_delimiter,
        "record_prefix": normalize_delimiter,
        "sep": normalize_delimiter,
    }
    return pd.json_normalize(profile_data, **normalize_config)


def save_profiling_data(profile_data: dict, conn_id: str = "bigquery"):
    """save profiling data to bigquery table

    :param profile_data: profiling data collected
    :param conn_id: Airflow's connection id to be used to publish the profiling data
    """
    db = create_database(conn_id)
    df = prep_profile_data(profile_data)
    sql = prep_insert_statement(df)
    db.run_sql(sql)


def profile(func, *args, **kwargs):  # noqa: C901
    def wrapper(*args, **kwargs):
        process = psutil.Process(os.getpid())
        # metrics before
        memory_full_info_before = process.memory_full_info()._asdict()
        cpu_time_before = process.cpu_times()._asdict()
        disk_usage_before = get_disk_usage()
        if sys.platform == "linux":
            io_counters_before = process.io_counters()._asdict()
        start = time.time()

        # run decorated function
        result = func(*args, **kwargs)

        # metrics after
        elapsed_time = elapsed_since(start)
        memory_full_info_after = process.memory_full_info()._asdict()
        cpu_time_after = process.cpu_times()._asdict()
        disk_usage_after = get_disk_usage()
        if sys.platform == "linux":
            io_counters_after = process.io_counters()._asdict()

        profile = {
            "duration": elapsed_time,
            "memory_full_info": subtract(
                memory_full_info_after, memory_full_info_before
            ),
            "cpu_time": subtract(cpu_time_after, cpu_time_before),
            "disk_usage": disk_usage_after - disk_usage_before,
        }
        if sys.platform == "linux":
            profile["io_counters"] = (subtract(io_counters_after, io_counters_before),)

        profile = {**profile, **kwargs}
        print(json.dumps(profile, default=str))
        if os.getenv("ASTRO_PUBLISH_BENCHMARK_DATA"):
            save_profiling_data(profile)
        return result

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


def build_dag_id(dataset, database):
    return f"load_file_{dataset}_into_{database}"


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
    dag_id = build_dag_id(args.dataset, args.database)
    run_dag(
        dag_id=dag_id,
        execution_date=timezone.utcnow(),
        revision=args.revision,
        chunk_size=args.chunk_size,
    )
