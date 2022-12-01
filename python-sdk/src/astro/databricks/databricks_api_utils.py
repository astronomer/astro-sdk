import pathlib
import time
from pathlib import Path
from typing import Dict

from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi
from requests.exceptions import HTTPError

from astro.databricks.autoloader.autoloader_file_generator import render

cwd = pathlib.Path(__file__).parent


def create_secrets(scope_name: str, filesystem_secrets: Dict[str, str], api_client: ApiClient):
    """
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
    try:
        secrets.delete_scope(scope_name)
    except HTTPError as h:
        # We expect a "resource does not exist if the scope has not been created. otherwise throw error
        if not h.response.json()["error_code"] == "RESOURCE_DOES_NOT_EXIST":
            raise h

    secrets.create_scope(
        scope=scope_name,
        initial_manage_principal=None,
        backend_azure_keyvault=None,
        scope_backend_type=None,
    )

    for k, v in filesystem_secrets.items():
        secrets.put_secret(scope=scope_name, key=k, string_value=v, bytes_value=None)


def generate_file(
    data_source_path: str,
    table_name: str,
    source_type: str,
    load_options: Dict[str, str],
    output_file_path: Path,
):
    """
    In order to run autoloader jobs in databricks, we need to generate a python file that creates a pyspark job.
    This function uses jinja templating to generate a file with all necessary user inputs
    :param data_source_path: The path where the data we're injesting lives (e.g. the s3 bucket URL)
    :param table_name: The delta table we would like to push the data into
    :param source_type: the source location (e.g. s3)
    :param load_options: any additional load options the user would like to inject into their job
    :param output_file_path: where the generated file should be placed.
    :return: output file path
    """
    render(
        Path("jinja_templates/autoload_file_to_delta.py.jinja2"),
        {
            "source_type": source_type,
            "table_name": table_name,
            "load_dict": load_options,
            "input_path": data_source_path,
        },
        output_file_path,
    )
    return output_file_path


def load_file_to_dbfs(local_file_path: Path, file_name: str, api_client: ApiClient) -> Path:
    """
    Load a file into DBFS. Used to move a python file into DBFS so we can run the jobs as pyspark jobs

    :param local_file_path: path of the file to upload
    :param api_client: Databricks API client
    """
    print("loading file " + str(local_file_path))
    dbfs = DbfsApi(api_client=api_client)
    file_path = DbfsPath("dbfs:/mnt/pyscripts/")
    dbfs.mkdirs(file_path)
    dbfs.delete(file_path.join(file_name), recursive=False)
    dbfs.put_file(src_path=str(local_file_path), dbfs_path=file_path.join(file_name), overwrite=True)
    return Path("dbfs:/mnt/pyscripts") / file_name


def create_and_run_job(
    api_client,
    file_to_run: str,
    existing_cluster_id=None,
    new_cluster_specs=None,
):
    """
    Creates a databricks job and runs it to completion.
    :param api_client: databricks API client
    :param existing_cluster_id: If you want to run this job on an existing cluster, you can give the cluster ID
    :param new_cluster_specs: If you want to run this job on a new cluster, you can give the cluster specs
        as a python dictionary.
    """
    jobs_api = JobsApi(api_client=api_client)
    json_info = {
        "name": "autoloader Python job S3 " + file_to_run,
        "spark_python_task": {"python_file": file_to_run},
    }
    if existing_cluster_id:
        json_info["existing_cluster_id"] = existing_cluster_id
    elif new_cluster_specs:
        json_info["new_cluster"] = new_cluster_specs

    a = jobs_api.create_job(json=json_info)

    run_id = jobs_api.run_now(
        job_id=a["job_id"],
        jar_params=None,
        notebook_params=None,
        python_params=None,
        spark_submit_params=None,
    )["run_id"]

    runs_api = RunsApi(api_client)
    while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "PENDING":
        print("job pending")
        time.sleep(5)

    while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "RUNNING":
        print("job running")
        time.sleep(3)

    print(f"final state: {runs_api.get_run(run_id)['state']['result_state']}")
