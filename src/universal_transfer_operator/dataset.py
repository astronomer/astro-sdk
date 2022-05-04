import logging
import warnings
from typing import TYPE_CHECKING, Any, List
from urllib.parse import unquote, urlparse

from airflow.exceptions import AirflowException
from airflow.providers_manager import ProvidersManager
from airflow.utils.module_loading import import_string

if TYPE_CHECKING:
    from airflow.models.connection import Connection  # Avoid circular imports.

from src.universal_transfer_operator.data_providers.base import DataProviders

log = logging.getLogger(__name__)


class Dataset:
    r"""
    The Dataset class refers to the pointer to the data and reference
    to it along with the connection details.

    The namespace for a dataset is the unique name for its dataset. The
    namespace and name of a dataset can be combined to
    form a URI (scheme:[//authority]path)
    * Namespace = scheme:[//authority] (the Dataset)
    * Name = path (the datasets)

    Broadly, Datasets can be of following categories:
    * Databases / Data Warehouses
    * File System / Blob storage
    * APIs

    ### Databases / Data Warehouses:
    Datasets are called tables. Tables are organised in databases and schemas.

    #### Postgres:
    Dataset hierarchy:
     * Host
     * Port

    Naming hierarchy:
     * Database
     * Schema
     * Table

    Identifier:
     * Namespace: postgres://{host}:{port} of the service instance.
       * Scheme = postgres
       * Authority = {host}:{port}
     * Unique name: {database}.{schema}.{table}
       * URI =  postgres://{host}:{port}/{database}.{schema}.{table}

    #### MySQL:
    Dataset hierarchy:
     * Host
     * Port

    Naming hierarchy:
     * Database
     * Table

    Identifier:
     * Namespace: mysql://{host}:{port} of the service instance.
       * Scheme = mysql
       * Authority = {host}:{port}
     * Unique name: {database}.{table}
       * URI =  mysql://{host}:{port}/{database}.{table}

       * URI =  mysql://{host}:{port}/{database}.{table}

    #### Redshift:
    Dataset hierarchy:
     * Host: examplecluster.<XXXXXXXXXXXX>.us-west-2.redshift.amazonaws.com
     * Port: 5439

    Naming hierarchy:
     * Database
     * Schema
     * Table

    Identifier:
     * Namespace: redshift://{host}:{port} of the cluster instance.
       * Scheme = redshift
       * Authority = {host}:{port}
     * Unique name: {database}.{schema}.{table}
       * URI =  redshift://{host}:{port}/{database}.{schema}.{table}

    #### Snowflake
    See: [Object Identifiers — Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/identifiers.html)

    Dataset hierarchy:
     * account name

    Naming hierarchy:
     * Database: {database name} => unique across the account
     * Schema: {schema name} => unique within the database
     * Table: {table name} => unique within the schema

    Database, schema, table or column names are uppercase in Snowflake.
    Clients should make sure that they are sending those as uppercase.

    Identifier:
     * Namespace: snowflake://{account name}
       * Scheme = snowflake
       * Authority = {account name}
     * Name: {database}.{schema}.{table}
       * URI =   snowflake://{account name}/{database}.{schema}.{table}

    #### BigQuery
    See:
    [Creating and managing projects | Resource Manager
    Documentation](https://cloud.google.com/resource-manager/docs/creating-managing-projects)
    [Introduction to datasets | BigQuery](https://cloud.google.com/bigquery/docs/datasets-intro)
    [Introduction to tables | BigQuery](https://cloud.google.com/bigquery/docs/tables-intro)

    Dataset hierarchy:
     * bigquery

    Naming hierarchy:
     * Project Name: {project name} => is not unique
     * Project number: {project number} => numeric: is unique across google cloud
     * Project ID: {project id} => readable: is unique across google cloud
     * dataset: {dataset name} => is unique within a project
     * table: {table name} => is unique within a dataset

    Identifier :
     * Namespace: bigquery
       * Scheme = bigquery
       * Authority =
     * Unique name: {project id}.{dataset name}.{table name}
       * URI =   bigquery://{project id}.{schema}.{table}

    #### Azure Synapse:
    Dataset hierarchy:
     * Host: <XXXXXXXXXXXX>.sql.azuresynapse.net
     * Port: 1433
     * Database: SQLPool1

    Naming hierarchy:
     * Schema
     * Table

    Identifier:
     * Namespace: sqlserver://{host}:{port};database={database};
       * Scheme = sqlserver
       * Authority = {host}:{port}
     * Unique name: {schema}.{table}
       * URI = sqlserver://{host}:{port};database={database}/{schema}.{table}

    ### File System / Blob storage
    #### LOCAL
    Naming hierarchy:
     * Path

    Identifier :
     * Namespace: file://
       * Scheme = file
       * Authority = ""
     * Unique name: {path}
       * URI =   file://{path}

    #### GCS
    Dataset hierarchy: none, naming is global

    Naming hierarchy:
     * bucket name => globally unique
     * Path

    Identifier :
     * Namespace: gs://{bucket name}
       * Scheme = gs
       * Authority = {bucket name}
     * Unique name: {path}
       * URI =   gs://{bucket name}{path}

    #### S3
    Naming hierarchy:
     * bucket name => globally unique
     * Path

    Identifier :
     * Namespace: s3://{bucket name}
       * Scheme = s3
       * Authority = {bucket name}
     * Unique name: {path}
       * URI =   s3://{bucket name}{path}

    #### HDFS
    Naming hierarchy:
     * Namenode: host + port
     * Path

    Identifier :
     * Namespace: hdfs://{namenode host}:{namenode port}
       * Scheme = hdfs
       * Authority = {namenode host}:{namenode port}
     * Unique name: {path}
       * URI =   hdfs://{namenode host}:{namenode port}{path}

    #### WASBS (Azure Blob Storage)
    Naming hierarchy:
     * service name => globally unique
     * Path

    Identifier :
     * Namespace: wasbs://{container name}@{service name}
       * Scheme = wasbs
       * Authority = service name
     * Unique name: {path}
       * URI =   wasbs://{container name}@{service name}{path}

    ### API
    Naming hierarchy:
     * endpoint => globally unique
     * header

    Identifier :
     * Namespace: api://
       * Scheme = api
       * Authority = ""
     * Unique name: {endpoint}
       * URI =   api://{endpoint}
    """

    def __init__(self, conn_id: str, uri: str) -> None:
        """
        :param conn_id: connection id
        :uri: URI as per naming hierarchy and naming scheme defined in doc-strings.
        """
        self.conn_id = conn_id
        self.uri = uri

    def _parse_from_uri(self) -> Any:
        """
        Returns the uri of the Dataset object.
        """
        uri_parts = urlparse(self.uri)
        conn_type = uri_parts.scheme

        self.conn_type = conn_type
        self.host = self._parse_netloc_to_hostname(uri_parts)
        quoted_schema = uri_parts.path[1:]
        self.schema = unquote(quoted_schema) if quoted_schema else quoted_schema
        self.login = (
            unquote(uri_parts.username) if uri_parts.username else uri_parts.username
        )
        self.password = (
            unquote(uri_parts.password) if uri_parts.password else uri_parts.password
        )
        self.port = uri_parts.port
        return uri_parts

    def _parse_netloc_to_hostname(self, uri_parts) -> str:
        """Parse a URI string to get correct Hostname."""
        hostname = unquote(uri_parts.hostname or "")
        if "/" in hostname:
            hostname = uri_parts.netloc
            if "@" in hostname:
                hostname = hostname.rsplit("@", 1)[1]
            if ":" in hostname:
                hostname = hostname.split(":", 1)[0]
            hostname = unquote(hostname)
        return hostname

    def get_connections(self) -> List["Connection"]:
        """
        Get all connections as an iterable, given the connection id.
        :return: array of connections
        """
        return [self.get_connection()]

    def get_connection(self) -> Connection:
        """
        Get connection, given connection id.
        :return: connection
        """
        from airflow.models.connection import Connection

        conn = Connection.get_connection_from_secrets(self.conn_id)
        log.info("Using connection ID '%s' for task execution.", conn.conn_id)
        return conn

    def get_provider(self) -> DataProviders:
        """Return DataProvider based on conn_type."""
        (
            hook_class_name,
            conn_id_param,
            package_name,
            hook_name,
            connection_type,
        ) = ProvidersManager().hooks.get(self.conn_type, (None, None, None, None, None))

        if not hook_class_name:
            raise AirflowException(f'Unknown hook type "{self.conn_type}"')
        try:
            hook_class = import_string(hook_class_name)
        except ImportError:
            warnings.warn(
                "Could not import %s when discovering %s %s",
                hook_class_name,
                hook_name,
                package_name,
            )
            raise
        return hook_class(**{conn_id_param: self.conn_id})
