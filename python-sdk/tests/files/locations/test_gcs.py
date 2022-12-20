from __future__ import annotations

from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from astro.files.locations import create_file_location
from astro.files.locations.google.gcs import GCSLocation, _pull_credentials_from_json_dict


@patch(
    "airflow.providers.google.cloud.hooks.gcs.GCSHook.list",
    return_value=["house1.csv", "house2.csv"],
)
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    location = create_file_location("gs://tmp/house")
    assert sorted(location.paths) == sorted(["gs://tmp/house1.csv", "gs://tmp/house2.csv"])


mock_creds = {"client_email": "foo", "project_id": "bar", "private_key": "baz", "private_key_id": "fizz"}


def test_pull_from_json_dict():
    with pytest.raises(ValueError) as exc_info:
        _pull_credentials_from_json_dict({"client_email": "foo", "project_id": "bar", "private_key": "baz"})
    assert exc_info.value.args[0] == "Error retrieving GCP credentials. missing key(s): private_key_id"
    working_json = _pull_credentials_from_json_dict(mock_creds)
    assert working_json == {
        "spark.hadoop.fs.gs.auth.service.account.email": "foo",
        "spark.hadoop.fs.gs.auth.service.account.private.key": "baz",
        "spark.hadoop.fs.gs.auth.service.account.private.key.id": "fizz",
        "spark.hadoop.fs.gs.project.id": "bar",
        "spark.hadoop.google.cloud.auth.service.account.enable": "true",
    }


@pytest.mark.parametrize("input_dict", [{}], ids=["empty_dict"])
def test_databricks_auth_no_value(input_dict):
    with mock.patch(
        "astro.files.locations.google.gcs.GCSLocation.hook", new_callable=PropertyMock
    ), pytest.raises(ValueError) as exc_info:
        mock_conn = MagicMock()
        mock_conn.extra_dejson = input_dict
        location = GCSLocation(path="gs://foo/bar", conn_id="mock_gcs_conn")
        location.hook.get_connection.return_value.extra_dejson = {}
        location.databricks_auth_settings()
    assert (
        exc_info.value.args[0]
        == "Error: to pull credentials from GCP We either need a keyfile or a keyfile_dict to retrieve credentials"
    )


@pytest.mark.parametrize(
    "input_dict", [{"key_path": "foo/bar"}, {"keyfile_dict": mock_creds}], ids=["key_path", "key_dict"]
)
def test_databricks_auth(input_dict):
    with mock.patch(
        "astro.files.locations.google.gcs.GCSLocation.hook", new_callable=PropertyMock
    ), mock.patch("astro.files.locations.google.gcs._pull_credentials_from_keypath") as mock_keypath:
        mock_conn = MagicMock()
        mock_conn.extra_dejson = input_dict
        location = GCSLocation(path="gs://foo/bar", conn_id="mock_gcs_conn")
        location.hook.get_connection.return_value.extra_dejson = input_dict
        location.databricks_auth_settings()
        if input_dict.get("key_path"):
            mock_keypath.assert_called_once()
        else:
            mock_keypath.assert_not_called()
