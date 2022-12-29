import sys
from enum import Enum

# typing.Literal was only introduced in Python 3.8, and we support Python 3.7
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal


class Location(Enum):
    LOCAL = "local"
    HTTP = "http"
    HTTPS = "https"
    GS = "gs"  # Google Cloud Storage
    GOOGLE_DRIVE = "gdrive"
    S3 = "s3"  # Amazon S3
    WASB = "wasb"  # Azure Blob Storage
    WASBS = "wasbs"  # Azure Blob Storage
    POSTGRES = "postgres"
    POSTGRESQL = "postgres"
    SQLITE = "sqlite"
    DELTA = "delta"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"


class FileLocation(Enum):
    # [START filelocation]
    LOCAL = "local"
    HTTP = "http"
    HTTPS = "https"
    GS = "gs"  # Google Cloud Storage
    GOOGLE_DRIVE = "gdrive"
    S3 = "s3"  # Amazon S3
    WASB = "wasb"  # Azure Blob Storage
    WASBS = "wasbs"  # Azure Blob Storage
    # [END filelocation]

    def __str__(self) -> str:
        return self.value


class IngestorSupported(Enum):
    # [START transferingestor]
    Fivetran = "fivetran"
    # [END transferingestor]

    def __str__(self) -> str:
        return self.value


class TransferMode(Enum):
    # [START TransferMode]
    NATIVE = "native"
    NONNATIVE = "nonnative"
    THIRDPARTY = "thirdparty"
    # [END TransferMode]

    def __str__(self) -> str:
        return self.value


class FileType(Enum):
    # [START filetypes]
    CSV = "csv"
    JSON = "json"
    NDJSON = "ndjson"
    PARQUET = "parquet"
    # [END filetypes]

    def __str__(self) -> str:
        return self.value


class Database(Enum):
    # [START database]
    POSTGRES = "postgres"
    POSTGRESQL = "postgres"
    SQLITE = "sqlite"
    DELTA = "delta"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    # [END database]

    def __str__(self) -> str:
        return self.value


SUPPORTED_FILE_LOCATIONS = [const.value for const in FileLocation]
SUPPORTED_FILE_TYPES = [const.value for const in FileType]
SUPPORTED_DATABASES = [const.value for const in Database]

LoadExistStrategy = Literal["replace", "append"]
