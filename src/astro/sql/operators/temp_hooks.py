"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import warnings
from typing import Dict, Optional, Sequence, Union
from urllib.parse import quote_plus

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class TempSnowflakeHook(SnowflakeHook):
    """
    Temporary class to get around a bug in the snowflakehook when creating URIs
    """

    def get_uri(self) -> str:
        """Override DbApiHook get_uri method for get_sqlalchemy_engine()"""
        conn_config = self._get_conn_params()
        uri = (
            "snowflake://{user}:{password}@{account}/{database}/{schema}"
            "?warehouse={warehouse}&role={role}&authenticator={authenticator}"
        )
        return uri.format(**conn_config)


class TempPostgresHook(PostgresHook):
    """
    Temporary class to get around a bug in the snowflakehook when creating URIs
    """

    def get_uri(self) -> str:
        """
        Extract the URI from the connection.

        :return: the extracted uri.
        """
        conn = self.get_connection(getattr(self, self.conn_name_attr))
        login = ""
        if conn.login:
            login = f"{quote_plus(conn.login)}:{quote_plus(conn.password)}@"
        host = conn.host
        if conn.port is not None:
            host += f":{conn.port}"
        uri = f"postgresql://{login}{host}/"
        if self.schema:
            uri += self.schema
        return uri


class TempBigQueryHook(BigQueryHook):
    def __init__(
        self,
        gcp_conn_id: str = GoogleBaseHook.default_conn_name,
        delegate_to: Optional[str] = None,
        use_legacy_sql: bool = True,
        location: Optional[str] = None,
        bigquery_conn_id: Optional[str] = None,
        api_resource_configs: Optional[Dict] = None,
        impersonation_chain: Optional[Union[str, Sequence[str]]] = None,
        labels: Optional[Dict] = None,
    ):
        """Override __init__() method of BigQueryHook since it was not passing gcp_conn_id param to DbApiHook correctly."""

        # To preserve backward compatibility
        # TODO: remove one day
        if bigquery_conn_id:
            warnings.warn(
                "The bigquery_conn_id parameter has been deprecated. You should pass "
                "the gcp_conn_id parameter.",
                DeprecationWarning,
                stacklevel=2,
            )
            gcp_conn_id = bigquery_conn_id
        GoogleBaseHook.__init__(
            self,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        DbApiHook.__init__(
            self,
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.running_job_id = None  # type: Optional[str]
        self.api_resource_configs = (
            api_resource_configs if api_resource_configs else {}
        )  # type Dict
        self.labels = labels
