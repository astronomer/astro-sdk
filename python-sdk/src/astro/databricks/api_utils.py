import fileinput
import logging
import os
import pathlib
import time
from pathlib import Path

from airflow.exceptions import AirflowException
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient

from astro.databricks.load_file.load_file_python_code_generator import render
from astro.databricks.load_options import DeltaLoadOptions

cwd = pathlib.Path(__file__).parent
log = logging.getLogger(__name__)


def generate_file(
    data_source_path: str,
    table_name: str,
    source_type: str,
    output_file_path: Path,
    load_options: DeltaLoadOptions,
    file_type: str = "",
):
    """
    In order to run autoloader jobs in databricks, we need to generate a python file that creates a pyspark job.
    This function uses jinja templating to generate a file with all necessary user inputs

    :param data_source_path: The path where the data we're injesting lives (e.g. the s3 bucket URL)
    :param table_name: The delta table we would like to push the data into
    :param source_type: the source location (e.g. s3)
    :param load_options: any additional load options the user would like to inject into their job
    :param output_file_path: where the generated file should be placed.
    :param file_type: when using the COPY INTO command, Spark requires explicitly stating the data type you are loading.
        For individual files we can infer the file type, but if you are loading a directory please explicitly state
        the file type in the File object.
    :return: output file path
    """
    render(
        Path("jinja_templates/load_file_to_delta.py.jinja2"),
        {
            "source_type": source_type,
            "table_name": table_name,
            "load_options": load_options,
            "input_path": data_source_path,
            "file_type": file_type.upper(),
        },
        output_file_path,
    )

    # Since we had to use jinja2 safe strings, we want to inline replace \' with '
    # This is not a security concern now that jinja2 is no longer running.
    for line in fileinput.input(output_file_path, inplace=True):
        print(line.replace("\\'", "'"), end="")
    log.debug("Created file %s", output_file_path)
    return output_file_path


def load_file_to_dbfs(local_file_path: Path, file_name: str, api_client: ApiClient) -> Path:
    # TODO we should allow arbitrary dbfs paths set by env vars/users
    """Load a file into DBFS. Used to move a python file into DBFS, so we can run the jobs as pyspark jobs.
    Currently saves the file to dbfs:/mnt/pyscripts/, but will eventually support more locations.

    :param local_file_path: path of the file to upload
    :param api_client: Databricks API client
    :return: path to the file in DBFS

    """
    log.debug("loading file %s", str(local_file_path))
    dbfs = DbfsApi(api_client=api_client)
    file_path = DbfsPath("dbfs:/mnt/pyscripts/")
    dbfs.mkdirs(file_path)
    dbfs.delete(file_path.join(file_name), recursive=False)
    dbfs.put_file(src_path=str(local_file_path), dbfs_path=file_path.join(file_name), overwrite=True)
    return Path("dbfs:/mnt/pyscripts") / file_name


def create_and_run_job(
    api_client,
    file_to_run: str,
    databricks_job_name: str,
    existing_cluster_id=None,
    new_cluster_specs=None,
) -> None:
    """Creates a databricks job and runs it to completion.

    :param databricks_job_name: Name of the job to submit
    :param file_to_run: path to file we will run with the job
    :param api_client: databricks API client
    :param existing_cluster_id: If you want to run this job on an existing cluster, you can give the cluster ID
    :param new_cluster_specs: If you want to run this job on a new cluster, you can give the cluster specs
        as a python dictionary.

    """
    jobs_api = JobsApi(api_client=api_client)
    json_info = {
        "name": databricks_job_name,
        "spark_python_task": {"python_file": file_to_run},
    }
    if existing_cluster_id:
        log.debug("Using existing cluster %s", existing_cluster_id)
        json_info["existing_cluster_id"] = existing_cluster_id
    elif new_cluster_specs:
        log.debug("creating new spark cluster with spec", new_cluster_specs)
        json_info["new_cluster"] = new_cluster_specs

    # TODO find a way to link to existing job so we're not creating a new job id every time
    job = jobs_api.create_job(json=json_info)
    log.debug("Created job %s", job["job_id"])
    run_id = jobs_api.run_now(
        job_id=job["job_id"],
        jar_params=None,
        notebook_params=None,
        python_params=None,
        spark_submit_params=None,
    )["run_id"]

    runs_api = RunsApi(api_client)
    ping_interval = int(os.getenv("AIRFLOW__ASTRO_SDK__DATABRICKS_PING_INTERVAL", "5"))
    while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "PENDING":
        log.info("job pending")
        time.sleep(ping_interval)

    while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "RUNNING":
        log.info("job running")
        time.sleep(ping_interval)
    final_job_state = runs_api.get_run(run_id)
    final_state = final_job_state["state"]["result_state"]
    log.info("final state: %s", {final_state})
    if final_state == "FAILED":
        raise AirflowException(f"Databricks job failed. Job info {final_job_state}")
