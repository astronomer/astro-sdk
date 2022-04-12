from astro import constants


class MissingPackage(type):
    """
    Base class used to handle missing packages.
    Raises an exception if the user attempts to access a class attribute.
    """

    package_name: str = ""
    related_extras: str = ""

    def __getattr__(cls, key: str) -> None:
        raise RuntimeError(
            f"Error loading the package {cls.package_name},"
            f" please make sure all the dependencies are installed."
            f" try - pip install {constants.PYPI_PROJECT_NAME}[{cls.related_extras}]"
        )


class GoogleMissingPackage(metaclass=MissingPackage):
    """Class used to represent missing dependencies related to Google services."""

    def __init__(self, *args, **kwargs):  # skipcq: PTC-W0049
        pass

    package_name = "apache-airflow-providers-google"
    related_extras = "google"


class AmazonMissingPackage(metaclass=MissingPackage):
    """Class used to represent missing dependencies related to Amazon services."""

    def __init__(self, *args, **kwargs):  # skipcq: PTC-W0049
        pass

    package_name = "apache-airflow-providers-amazon"
    related_extras = "amazon"


class SnowflakeMissingPackage(metaclass=MissingPackage):
    """Class used to represent missing dependencies related to Snowflake."""

    package_name = "apache-airflow-providers-snowflake"
    related_extras = "snowflake"


class SnowflakePandasMissingPackage(metaclass=MissingPackage):
    """Class used to represent missing dependencies related to Snowflake & Pandas."""

    package_name = "snowflake-connector-python[pandas]"
    related_extras = "snowflake"


class PostgresMissingPackage(metaclass=MissingPackage):
    """Class used to represent missing dependencies related to Postgres."""

    package_name = "apache-airflow-providers-postgres"
    related_extras = "postgres"


try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    from psycopg2 import sql as postgres_sql
except ModuleNotFoundError:
    PostgresHook = PostgresMissingPackage
    postgres_sql = PostgresMissingPackage

try:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
except ModuleNotFoundError:
    SnowflakeHook = SnowflakeMissingPackage

try:
    from snowflake.connector import pandas_tools
except ModuleNotFoundError:
    pandas_tools = SnowflakePandasMissingPackage

try:
    from airflow.providers.amazon.aws.hooks import base_aws, s3  # skipcq: PYL-C0412
    from boto3 import Session as BotoSession
except ModuleNotFoundError:
    s3 = AmazonMissingPackage
    AwsBaseHook = AmazonMissingPackage
    BotoSession = AmazonMissingPackage
else:
    AwsBaseHook = base_aws.AwsBaseHook  # type: ignore

try:
    from airflow.providers.google.cloud.hooks import gcs  # skipcq: PYL-C0412
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
    from google.cloud import bigquery
    from google.cloud.storage import Client as GCSClient
    from google.oauth2 import service_account as google_service_account
except ModuleNotFoundError:
    BigQueryHook = GoogleMissingPackage
    bigquery = GoogleMissingPackage
    GCSClient = GoogleMissingPackage
    GCSHook = GoogleMissingPackage
    gcs = GoogleMissingPackage
    google_service_account = GoogleMissingPackage
else:
    GCSHook = gcs.GCSHook  # type: ignore
