import sys
from enum import Enum

# typing.Literal was only introduced in Python 3.8, and we support Python 3.7
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

DEFAULT_CHUNK_SIZE = 1000000
PYPI_PROJECT_NAME = "astro-sdk-python"


class FileLocation(Enum):
    LOCAL = "local"
    HTTP = "http"
    HTTPS = "https"
    GS = "gs"  # Google Cloud Storage
    S3 = "s3"  # Amazon S3


class FileType(Enum):
    CSV = "csv"
    JSON = "json"
    NDJSON = "ndjson"
    PARQUET = "parquet"


class Database(Enum):
    POSTGRES = "postgres"
    POSTGRESQL = "postgres"
    SQLITE = "sqlite"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"


SUPPORTED_FILE_LOCATIONS = [const.value for const in FileLocation]
SUPPORTED_FILE_TYPES = [const.value for const in FileType]
SUPPORTED_DATABASES = [const.value for const in Database]

LoadExistStrategy = Literal["replace", "append"]

ExportExistsStrategy = Literal["replace", "exception"]

# TODO: check how snowflake names these
MergeConflictStrategy = Literal["ignore", "update", "exception"]

LOAD_DATAFRAME_ERROR_MESSAGE = (
    "Failing this task because you do not have a custom xcom backend set up. If you use "
    "the default XCOM backend to store large dataframes, this can significantly degrade "
    "Airflow DB performance. Please set up a custom XCOM backend (info here "
    "https://www.astronomer.io/guides/custom-xcom-backends) or set the environment "
    "variable AIRFLOW__ASTRO_SDK__DATAFRAME_ALLOW_UNSAFE_STORAGE to true if you wish to proceed while "
    "knowing the risks. "
)
