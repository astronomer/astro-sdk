from enum import Enum


class SupportedDataProvider(Enum):
    """
    List of all the supported DataProviders.
    """

    LOCAL = "local"
    S3 = "s3"
    FTP = "ftp"
    HDFS = "hdfs"
    SFTP = "sftp"
    GCS = "gcs"
    MYSQL = "mysql"
    POSTGRES = "postgres"
    BIGQUERY = "bigquery"
    REDSHIFT = "redshift"


class Database(Enum):
    """
    List of all the supported Databases DataProviders
    """

    POSTGRES = "postgres"
    POSTGRESQL = "postgres"
    SQLITE = "sqlite"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"


class FileSystem(Enum):
    """
    List of all the supported FileSystem DataProviders
    """

    LOCAL = "local"
    GCS = "gcs"  # Google Cloud Storage
    S3 = "s3"  # Amazon S3


class API(Enum):
    """
    List of all the supported API DataProviders
    """

    HTTP = "http"
    HTTPS = "https"


class SupportedFileType(Enum):
    CSV = "csv"
    JSON = "json"
    NDJSON = "ndjson"
    PARQUET = "parquet"


SUPPORTED_FILE_LOCATIONS = [const.value for const in FileSystem]
SUPPORTED_FILE_TYPES = [const.value for const in SupportedFileType]
SUPPORTED_DATABASES = list({const.value for const in Database})

CONN_TYPE_TO_DATABASE = {
    "postgres": Database.POSTGRES,
    "postgresql": Database.POSTGRES,
    "sqlite": Database.SQLITE,
    "bigquery": Database.BIGQUERY,
    "gcpbigquery": Database.BIGQUERY,
    "google_cloud_platform": Database.BIGQUERY,
    "snowflake": Database.SNOWFLAKE,
}
