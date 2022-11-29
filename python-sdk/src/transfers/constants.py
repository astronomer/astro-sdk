from enum import Enum


class FileLocation(Enum):
    # [START filelocation]
    LOCAL = "local"
    HTTP = "http"
    HTTPS = "https"
    GS = "google_cloud_platform"  # Google Cloud Storage
    google_cloud_platform = "google_cloud_platform"  # Google Cloud Storage
    S3 = "s3"  # Amazon S3
    AWS = "aws"
    # [END filelocation]


class IngestorSupported(Enum):
    # [START transferingestor]
    Fivetran = "fivetran"
    # [END transferingestor]


class TransferMode(Enum):
    # [START TransferMode]
    NATIVE = "native"
    NONNATIVE = "nonnative"
    THIRDPARTY = "thirdparty"
    # [END TransferMode]
