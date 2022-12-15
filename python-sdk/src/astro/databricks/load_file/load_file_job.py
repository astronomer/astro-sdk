from __future__ import annotations

import pathlib
import tempfile
from pathlib import Path
from typing import Any

from databricks_cli.sdk.api_client import ApiClient

from astro.constants import FileLocation, LoadExistStrategy
from astro.databricks.api_utils import (
    create_and_run_job,
    create_secrets,
    delete_secret_scope,
    generate_file,
    load_file_to_dbfs,
)
from astro.databricks.load_options import DeltaLoadOptions
from astro.files import File
from astro.table import BaseTable

cwd = pathlib.Path(__file__).parent

supported_file_locations = [FileLocation.LOCAL, FileLocation.S3, FileLocation.GS]


def load_file_to_delta(
    input_file: File,
    delta_table: BaseTable,
    databricks_job_name: str,
    delta_load_options: DeltaLoadOptions,
    if_exists: LoadExistStrategy = "replace",
):
    """
    Load a file object into a databricks delta table

    :param if_exists: How to handle the data if the table exists. Defaults to replace (but can be set to "append")
    :param delta_load_options: Loading options for passing data into delta. Things like COPY_OPTIONS, cluster_id, etc.
    :param databricks_job_name: The name of the job as it will show up in databricks.
    :param input_file: File to load into delta
    :param delta_table: a Table object with necessary metadata for accessing the cluster.
    """
    from astro.databricks.delta import DeltaDatabase

    delta_load_options.if_exists = if_exists
    db = DeltaDatabase(conn_id=delta_table.conn_id)
    api_client = db.api_client
    if input_file.location.location_type not in supported_file_locations:
        raise ValueError(
            f"File location {input_file.location.location_type} not yet supported. "
            f"Currently supported locations: {','.join([str(s) for s in supported_file_locations])}"
        )
    dbfs_file_path = None
    if input_file.is_local():
        # There are no secrets to load since this is a local file
        delta_load_options.load_secrets = False
        dbfs_file_path = _load_local_file_to_dbfs(api_client, input_file)
    elif delta_load_options.load_secrets:
        delete_secret_scope(delta_load_options.secret_scope, api_client=api_client)
        _load_secrets_to_databricks(api_client, input_file, delta_load_options.secret_scope)

    with tempfile.NamedTemporaryFile(suffix=".py") as tfile:
        delta_load_options.if_exists = if_exists
        dbfs_file_path = _create_load_file_pyspark_file(
            api_client, delta_load_options, dbfs_file_path, delta_table, input_file, tfile
        )
    try:
        create_and_run_job(
            api_client=api_client,
            databricks_job_name=databricks_job_name,
            existing_cluster_id=delta_load_options.existing_cluster_id,
            file_to_run=str(dbfs_file_path),
        )
    finally:
        if input_file.location.location_type != FileLocation.LOCAL:
            if delta_load_options.secret_scope == DeltaLoadOptions.get_default_delta_options().secret_scope:
                delete_secret_scope(delta_load_options.secret_scope, api_client=api_client)


def _create_load_file_pyspark_file(
    api_client: ApiClient,
    databricks_options: DeltaLoadOptions,
    dbfs_file_path: Path | None,
    delta_table: BaseTable,
    input_file: File,
    output_file: Any,
):
    file_type = _find_file_type(input_file)
    file_path = generate_file(
        data_source_path=str(dbfs_file_path) if dbfs_file_path else input_file.path,
        table_name=delta_table.name,
        source_type=str(input_file.location.location_type),
        output_file_path=Path(output_file.name),
        file_type=file_type or "",
        load_options=databricks_options,
    )
    dbfs_file_path = load_file_to_dbfs(
        file_path, file_name=f"load_file_{delta_table.name}.py", api_client=api_client
    )
    return dbfs_file_path


def _load_local_file_to_dbfs(api_client: ApiClient, input_file: File):
    file_path = Path(input_file.path)
    dbfs_file_path = load_file_to_dbfs(
        local_file_path=Path(input_file.path), file_name=file_path.name, api_client=api_client
    )
    return dbfs_file_path


def _load_secrets_to_databricks(api_client: ApiClient, input_file: File, secret_scope_name: str):
    """
    Load secrets into a secret scope in databricks. This can be used as a utility for loading
    necessary secrets for accessing external data stores.

    :param api_client: Databricks API client loaded with credentials
    :param input_file: file for loading connection info
    :param secret_scope_name: name of the secret scope to load info into
    """
    create_secrets(
        scope_name=secret_scope_name,
        filesystem_secrets=input_file.location.databricks_auth_settings(),
        api_client=api_client,
    )


def _find_file_type(input_file: File) -> str:
    """
    Since COPY INTO does not automatically detect schema, we need to ensure that the file type is
    either set by the user, or inferable in the URL
    :param input_file: file to parse
    :return: file type of the uploaded file
    """
    if input_file.filetype:
        return str(input_file.filetype)
    if not input_file.filetype:
        if "." in input_file.path:
            return input_file.path.split(".")[-1]
    raise ValueError("For COPY INTO, you need to supply a file type.")
