import pathlib
import tempfile
from pathlib import Path

from astro.databricks.databricks_api_utils import (
    create_and_run_job,
    create_secrets,
    delete_secret_scope,
    generate_file,
    load_file_to_dbfs,
)
from astro.files import File
from astro.table import BaseTable

cwd = pathlib.Path(__file__).parent


def load_file_to_delta(input_file: File, delta_table: BaseTable):
    """
    Load a file object into a databricks delta table
    :param input_file: File to load into delta
    :param delta_table: a Table object with necessary metadata for accessing the cluster.
    """
    from astro.databricks.delta import DeltaDatabase

    db = DeltaDatabase(conn_id=delta_table.conn_id)
    api_client = db.api_client
    secret_scope_name = f"secret-scope-{delta_table.name}"
    create_secrets(
        secret_scope_name,
        filesystem_secrets=input_file.location.autoloader_config,
        api_client=api_client,
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

        dbfs_file_path = load_file_to_dbfs(
            file_path, file_name=f"load_file_{delta_table.name}.py", api_client=api_client
        )
    try:
        create_and_run_job(
            api_client=api_client, existing_cluster_id="1028-033729-pgbj7n9x", file_to_run=str(dbfs_file_path)
        )
    finally:
        delete_secret_scope(scope_name=secret_scope_name, api_client=api_client)
