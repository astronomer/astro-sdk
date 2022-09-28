from __future__ import annotations

import sys
from enum import Enum

# typing.Literal was only introduced in Python 3.8, and we support Python 3.7
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

DEFAULT_SCHEMA = "tmp_astro"
DEFAULT_CHUNK_SIZE = 1000000
PYPI_PROJECT_NAME = "astro-sdk-python"


class FileLocation(Enum):
    # [START filelocation]
    LOCAL = "local"
    HTTP = "http"
    HTTPS = "https"
    GS = "gs"  # Google Cloud Storage
    S3 = "s3"  # Amazon S3
    # [END filelocation]

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

ExportExistsStrategy = Literal["replace", "exception"]

# TODO: check how snowflake names these
MergeConflictStrategy = Literal["ignore", "update", "exception"]

ColumnCapitalization = Literal["upper", "lower", "original"]
