from enum import Enum

DEFAULT_CHUNK_SIZE = 1000000
PYPI_PROJECT_NAME = "astro-projects"
DEFAULT_SCHEMA = "tmp_astro"

LOAD_DATAFRAME_BYTES_LIMIT = 512000  # takes < 3 seconds
LOAD_COLUMN_AUTO_DETECT_ROWS = 1000


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


SUPPORTED_FILE_LOCATIONS = [const.value for const in FileLocation]
SUPPORTED_FILE_TYPES = [const.value for const in FileType]

UNIQUE_TABLE_NAME_LENGTH = 63
