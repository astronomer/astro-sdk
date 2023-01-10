from __future__ import annotations

import json
import os
from urllib.parse import urlparse, urlunparse

import smart_open
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from google.api_core.exceptions import NotFound

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


def _pull_credentials_from_json_dict(creds_json: dict) -> dict:
    expected_values = {"client_email", "project_id", "private_key", "private_key_id"}
    missing_values = expected_values - set(creds_json.keys())
    if missing_values:
        raise ValueError(f"Error retrieving GCP credentials. missing key(s): {','.join(missing_values)}")
    return {
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
        "spark.hadoop.fs.gs.auth.service.account.email": creds_json["client_email"],
        "spark.hadoop.fs.gs.project.id": creds_json["project_id"],
        "spark.hadoop.fs.gs.auth.service.account.private.key": creds_json["private_key"],
        "spark.hadoop.fs.gs.auth.service.account.private.key.id": creds_json["private_key_id"],
    }


def _pull_credentials_from_keypath(creds_dict: dict) -> dict:
    with open(creds_dict["key_path"]) as f:
        return _pull_credentials_from_json_dict(json.loads(f.read()))


class GCSLocation(BaseFileLocation):
    """Handler GS object store operations"""

    location_type = FileLocation.GS
    # TODO: Restrict the supported conn_type to only GCSHook.conn_type
    supported_conn_type = {GCSHook.conn_type, "gcpbigquery", "bigquery"}

    @property
    def hook(self) -> GCSHook:
        return GCSHook(gcp_conn_id=self.conn_id) if self.conn_id else GCSHook()

    @property
    def transport_params(self) -> dict:
        """get GCS credentials for storage"""
        client = self.hook.get_conn()
        return {"client": client}

    @property
    def paths(self) -> list[str]:
        """Resolve GS file paths with prefix"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        prefix = url.path[1:]
        prefixes = self.hook.list(bucket_name=bucket_name, prefix=prefix)
        paths = [urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes]
        return paths

    @property
    def size(self) -> int:
        """Return file size for GCS location"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        object_name = url.path
        if object_name.startswith("/"):
            object_name = object_name[1:]
        return int(self.hook.get_size(bucket_name=bucket_name, object_name=object_name))

    def databricks_auth_settings(self) -> dict:
        """
        Required settings to upload this file into databricks. Only needed for cloud storage systems
        like S3
        :return: A dictionary of settings keys to settings values
        """
        creds_dict = self.hook.get_connection(self.conn_id).extra_dejson
        if creds_dict.get("key_path"):
            return _pull_credentials_from_keypath(creds_dict=creds_dict)
        elif creds_dict.get("keyfile_dict"):
            return _pull_credentials_from_json_dict(creds_dict.get("keyfile_dict"))
        elif os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            return _pull_credentials_from_keypath(
                {"key_path": os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")}
            )
        else:
            raise ValueError(
                "Error: to pull credentials from GCP We either need a keyfile or a keyfile_dict "
                "to retrieve credentials"
            )

    def exists(self) -> bool:
        """Check if the file exists or not"""
        try:
            with smart_open.open(self.smartopen_uri, mode="r", transport_params=self.transport_params):
                return True
        except (OSError, NotFound):
            return False

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        parsed_url = urlparse(self.path)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return urlparse(self.path).path
