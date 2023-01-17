import fileinput
import logging
import pathlib
import time
from pathlib import Path
from typing import Dict

from airflow.configuration import conf
from airflow.exceptions import AirflowException
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi
from requests.exceptions import HTTPError

from astro.databases.databricks.load_file.load_file_python_code_generator import render
from astro.databases.databricks.load_options import DeltaLoadOptions

cwd = pathlib.Path(__file__).parent
log = logging.getLogger(__name__)


def delete_secret_scope(scope_name: str, api_client: ApiClient) -> None:
    """
    Delete the scope we created to prevent littering the databricks secret store with one-time scopes.

    :param scope_name: name of scope to delete
    :param api_client: populated databricks client
    """
    secrets = SecretApi(api_client)
    try:
        secrets.delete_scope(scope_name)
    except HTTPError as http_error:
        # We don't care if the resource doesn't exist since we're trying to delete it
        if not http_error.response.json().get("error_code", "") == "RESOURCE_DOES_NOT_EXIST":
            raise http_error


def create_secrets(scope_name: str, filesystem_secrets: Dict[str, str], api_client: ApiClient) -> None:
    """
    Uploads secrets to a scope
    Before we can transfer data from external file sources (s3, GCS, etc.) we first need to upload the relevant
    secrets to databricks, so we can use them in the autoloader config. This allows us to perform ad-hoc queries
    that are not dependent on existing settings.

    :param scope_name: the name of the secret scope. placing all secrets in a single scope makes them easy to
        delete later
    :param filesystem_secrets: a dictionary of k,v secrets where the key is the secret name and the value is
        the secret value
    :param api_client: The databricks API client that has all necessary credentials
    """
    secrets = SecretApi(api_client)
    secrets.create_scope(
        scope=scope_name,
        initial_manage_principal=None,
        backend_azure_keyvault=None,
        scope_backend_type=None,
    )

    for k, v in filesystem_secrets.items():
        # Add "astro_sdk_" to the front, so we know which secrets are hadoop settings
        secret_key = "astro_sdk_" + k
        secrets.put_secret(scope=scope_name, key=secret_key, string_value=v, bytes_value=None)


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
            "if_exists": load_options.if_exists,
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
    runs_api = RunsApi(api_client=api_client)
    json_info = {
        "name": databricks_job_name,
        "spark_python_task": {"python_file": file_to_run},
    }

    if new_cluster_specs:
        log.debug("creating new spark cluster with spec %s", new_cluster_specs)
        json_info["new_cluster"] = new_cluster_specs
    elif existing_cluster_id:
        log.debug("Using existing cluster %s", existing_cluster_id)
        json_info["existing_cluster_id"] = existing_cluster_id
    else:
        raise AirflowException("Error, must have a new cluster spec or an existing cluster ID for spark job")

    run_id = runs_api.submit_run(json=json_info)["run_id"]

    ping_interval = int(conf.get("astro_sdk", "databricks_ping_interval", fallback=5))
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
