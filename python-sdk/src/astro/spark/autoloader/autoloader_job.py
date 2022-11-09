import datetime
import pathlib
import tempfile
import time
from pathlib import Path

from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi

from astro.files import File
from astro.spark.autoloader.autoloader_file_generator import render
from astro.table import BaseTable

cwd = pathlib.Path(__file__).parent


def create_secrets(scope_name: str, filesystem_secrets: dict[str, str], api_client: ApiClient):
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
        secrets.create_scope(
            scope=scope_name,
            initial_manage_principal=None,
            backend_azure_keyvault=None,
            scope_backend_type=None,
        )
    except Exception:
        print("oh well it exists already")

    for k, v in filesystem_secrets.items():
        secrets.put_secret(scope=scope_name, key=k, string_value=v, bytes_value=None)


def generate_file(
    data_source_path: str,
    table_name: str,
    source_type: str,
    load_options: dict[str, str],
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


def load_file_to_dbfs(local_file_path: Path, api_client: ApiClient):
    """
    Load a file into DBFS. Used to move a python file into DBFS so we can run the jobs as pyspark jobs

    :param local_file_path: path of the file to upload
    :param api_client: Databricks API client
    """
    print("loading file " + str(local_file_path))
    dbfs = DbfsApi(api_client=api_client)
    file_path = DbfsPath("dbfs:/mnt/pyscripts/")
    dbfs.mkdirs(file_path)
    dbfs.delete(file_path.join("autoload_file_to_delta.py"), recursive=False)
    dbfs.put_file(
        src_path=str(local_file_path), dbfs_path=file_path.join("autoload_file_to_delta.py"), overwrite=True
    )


def create_and_run_job(
    api_client,
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
        "name": "autloader Python job S3 " + str(datetime.datetime.now()),
        "spark_python_task": {"python_file": "dbfs:/mnt/pyscripts/autoload_file_to_delta.py"},
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


def load_file_to_delta(input_file: File, delta_table: BaseTable):
    """
    Load a file object into a databricks delta table
    :param input_file: File to load into delta
    :param delta_table: a Table object with necessary metadata for accessing the cluster.
    """
    from astro.spark.delta import DeltaDatabase

    db = DeltaDatabase(conn_id=delta_table.conn_id)
    api_client = db.api_client()
    create_secrets(
        "my-scope", filesystem_secrets=input_file.location.autoloader_config(), api_client=api_client
    )

    with tempfile.NamedTemporaryFile(suffix=".py") as tfile:
        file_path = generate_file(
            data_source_path=input_file.path,
            table_name=delta_table.name,
            source_type=str(input_file.location.location_type),
            load_options={
                "cloudFiles.format": "csv",
                "header": "True",
            },
            output_file_path=Path(tfile.name),
        )

        load_file_to_dbfs(file_path, api_client=api_client)

    create_and_run_job(api_client=api_client, existing_cluster_id="1028-033729-pgbj7n9x")
