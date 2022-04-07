from .base import BaseDB


class BigQuery(BaseDB):

    # Connection types
    conn_types = ["bigquery", "gcpbigquery", "google_cloud_platform"]
