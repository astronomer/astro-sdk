from __future__ import annotations

import pathlib
from unittest import mock
from unittest.mock import call

from astro.databricks.api_utils import (
    create_and_run_job,
    create_secrets,
    delete_secret_scope,
    load_file_to_dbfs,
)

CWD = pathlib.Path(__file__).parent


@mock.patch("databricks_cli.sdk.api_client.ApiClient")
def test_create_and_run_job(mock_api_client):
    create_and_run_job(
        api_client=mock_api_client,
        file_to_run="/foo/bar.py",
        databricks_job_name="my-db-job",
        existing_cluster_id="foobar",
    )
    mock_api_client.perform_query.__getitem__("job_id").return_value = 123
    calls = [
        call.perform_query(
            "POST",
            "/jobs/create",
            data={
                "name": "my-db-job",
                "spark_python_task": {"python_file": "/foo/bar.py"},
                "existing_cluster_id": "foobar",
            },
            headers=None,
            version=None,
        ),
    ]
    mock_api_client.assert_has_calls(calls)


@mock.patch("databricks_cli.sdk.api_client.ApiClient")
def test_load_to_dbfs(mock_api_client):
    load_file_to_dbfs(
        local_file_path=CWD / "__init__.py", file_name="my_table.py", api_client=mock_api_client
    )
    calls = [
        call.perform_query("POST", "/dbfs/mkdirs", data={"path": "dbfs:/mnt/pyscripts/"}, headers=None),
        call.perform_query(
            "POST",
            "/dbfs/delete",
            data={"path": "dbfs:/mnt/pyscripts/my_table.py", "recursive": False},
            headers=None,
        ),
        call.perform_query(
            "POST",
            "/dbfs/put",
            data={"path": "dbfs:/mnt/pyscripts/my_table.py", "overwrite": True},
            headers={"Content-Type": None},
            files={"file": mock.ANY},
        ),
    ]
    mock_api_client.assert_has_calls(calls)


@mock.patch("databricks_cli.sdk.api_client.ApiClient")
def test_create_secrets(mock_api_client):
    create_secrets(scope_name="my-scope", filesystem_secrets={"foo": "bar"}, api_client=mock_api_client)
    calls = [
        call.perform_query(
            "POST",
            "/secrets/scopes/create",
            data={"scope": "my-scope"},
            headers=None,
        ),
        call.perform_query(
            "POST",
            "/secrets/put",
            data={"scope": "my-scope", "key": "astro_sdk_foo", "string_value": "bar"},
            headers=None,
        ),
    ]
    mock_api_client.assert_has_calls(calls)


@mock.patch("databricks_cli.sdk.api_client.ApiClient")
def test_delete_secret_scope(mock_api_client):
    delete_secret_scope(scope_name="my-scope", api_client=mock_api_client)
    mock_api_client.assert_has_calls(
        [
            call.perform_query(
                "POST",
                "/secrets/scopes/delete",
                data={"scope": "my-scope"},
                headers=None,
            )
        ]
    )
