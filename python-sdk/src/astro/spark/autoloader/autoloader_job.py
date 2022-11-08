import datetime
import pathlib
import tempfile
import time
from pathlib import Path

from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.jobs.api import JobsApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.secrets.api import SecretApi

from astro.files import File
from astro.spark.autoloader.autoloader_file_generator import render
from astro.table import BaseTable
cwd = pathlib.Path(__file__).parent


def create_secrets(scope_name, filesystem_secrets, api_client):
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


def generate_file(data_source_path, table_name, source_type, load_options, output_file_path):
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


def load_file_to_dbfs(local_file_path, api_client):
    print("loading file " + str(local_file_path))
    dbfs = DbfsApi(api_client=api_client)
    file_path = DbfsPath("dbfs:/mnt/pyscripts/")
    dbfs.mkdirs(file_path)
    dbfs.delete(file_path.join("autoload_file_to_delta.py"), recursive=False)
    dbfs.put_file(
        src_path=str(local_file_path), dbfs_path=file_path.join("autoload_file_to_delta.py"), overwrite=True
    )


def create_job(
    table: BaseTable,
    api_client,
    existing_cluster_id=None,
    new_cluster_specs=None,
):
    jobs_api = JobsApi(api_client=api_client)
    json_info = {
        "name": "autloader Python job S3 " + str(datetime.datetime.now()),
        "existing_cluster_id": "1028-033729-pgbj7n9x",
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

    create_job(table=delta_table, api_client=api_client)
