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
from urllib.parse import quote_plus

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
            "snowflake://{user}:{password}@{account}.{region}/{database}/{schema}"
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
