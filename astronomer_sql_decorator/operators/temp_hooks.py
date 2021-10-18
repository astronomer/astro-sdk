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
