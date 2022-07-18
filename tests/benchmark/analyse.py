#!/usr/bin/env python3

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
from google.cloud import storage

SUMMARY_FIELDS = [
    "database",
    "dataset",
    "total_time",
    "memory_rss",
    "cpu_time_user",
    "cpu_time_system",
]

if sys.platform == "linux":
    SUMMARY_FIELDS.append("memory_pss")
    SUMMARY_FIELDS.append("memory_shared")


def format_bytes(bytes_):
    if abs(bytes_) < 1000:
        return str(bytes_) + "B"
    elif abs(bytes_) < 1e6:
        return str(round(bytes_ / 1e3, 2)) + "kB"
    elif abs(bytes_) < 1e9:
        return str(round(bytes_ / 1e6, 2)) + "MB"
    else:
        return str(round(bytes_ / 1e9, 2)) + "GB"


def format_time(time):
    if time < 1:
        return str(round(time * 1000, 2)) + "ms"
    if time < 60:
        return str(round(time, 2)) + "s"
    if time < 3600:
        return str(round(time / 60, 2)) + "min"
    else:
        return str(round(time / 3600, 2)) + "hrs"


def check_gcs_path(results_filepath):
    url_obj = urlparse(results_filepath)

    if url_obj.scheme == "gs":
        return True
    return False


def download_files_from_gcs(results_filepath):
    """Downloads folder from the GCS bucket."""

    gs_url = urlparse(results_filepath)
    bucket_name = gs_url.netloc
    source_blob_name = gs_url.path

    local_destination_file_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        source_blob_name[1:],
    )
    if os.path.exists(os.path.dirname(local_destination_file_path)):
        os.remove(local_destination_file_path)
    else:
        os.mkdir(os.path.dirname(local_destination_file_path))
    local_file = Path(local_destination_file_path)
    local_file.touch(exist_ok=True)

    storage_client = storage.Client()

    bucket = storage_client.bucket(bucket_name)

    blob = bucket.blob(source_blob_name[1:])

    blob.download_to_filename(local_destination_file_path)

    print(
        "Downloaded storage object {} from bucket {} to local file {}.".format(
            source_blob_name, bucket_name, local_destination_file_path
        )
    )

    return local_destination_file_path


def analyse_results(results_filepath):
    data = []
    with open(results_filepath) as fp:
        for line in fp.readlines():
            data.append(json.loads(line.strip()))

    df = pd.json_normalize(data, sep="_")

    # calculate total CPU from process & children
    mean_by_dag = df.groupby("dag_id", as_index=False).mean()

    # format data
    mean_by_dag["database"] = mean_by_dag.dag_id.apply(
        lambda text: text.split("into_")[-1]
    )
    mean_by_dag["dataset"] = mean_by_dag.dag_id.apply(
        lambda text: text.split("load_file_")[-1].split("_into")[0]
    )

    mean_by_dag["memory_rss"] = mean_by_dag.memory_full_info_rss.apply(
        lambda value: format_bytes(value)
    )
    if sys.platform == "linux":
        mean_by_dag["memory_pss"] = mean_by_dag.memory_full_info_pss.apply(
            lambda value: format_bytes(value)
        )
        mean_by_dag["memory_shared"] = mean_by_dag.memory_full_info_shared.apply(
            lambda value: format_bytes(value)
        )

    mean_by_dag["total_time"] = mean_by_dag["duration"].apply(
        lambda ms_time: format_time(ms_time)
    )

    mean_by_dag["cpu_time_system"] = (
        mean_by_dag["cpu_time_system"] + mean_by_dag["cpu_time_children_system"]
    ).apply(lambda ms_time: format_time(ms_time))
    mean_by_dag["cpu_time_user"] = (
        mean_by_dag["cpu_time_user"] + mean_by_dag["cpu_time_children_user"]
    ).apply(lambda ms_time: format_time(ms_time))

    summary = mean_by_dag[SUMMARY_FIELDS]

    # print Markdown tables per database
    for database_name in summary["database"].unique().tolist():
        print(f"\n### Database: {database_name}\n")
        print(summary[summary["database"] == database_name].to_markdown(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Trigger benchmark DAG")
    parser.add_argument(
        "--results-filepath",
        "-r",
        type=str,
        help="NDJSON local path(/path/to/file.ndjson) or Google "
        "cloud storage path (gs://buckey/sample.ndjson) containing the results for a benchmark run",
    )
    args = parser.parse_args()
    results_filepath = args.results_filepath
    print(f"Running the analysis on {results_filepath}...")
    if check_gcs_path(results_filepath):
        results_filepath = download_files_from_gcs(results_filepath)
    analyse_results(results_filepath)
