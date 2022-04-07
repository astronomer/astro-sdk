from functools import cached_property

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

from astronew.constants import DEFAULT_CHUNK_SIZE

from .base import BaseDB


class BigQuery(BaseDB):

    # Connection types
    conn_types = ["bigquery", "gcpbigquery", "google_cloud_platform"]

    @cached_property
    def hook(self):
        _hook = BigQueryHook(use_legacy_sql=False, gcp_conn_id=self.conn_id)
        if self.database:
            _hook.database = self.database
        return _hook

    def load_pandas_dataframe(
        self,
        pandas_dataframe: pd.DataFrame,
        chunksize: int = DEFAULT_CHUNK_SIZE,
        if_exists: str = "replace",
    ):
        self.create_schema_if_needed()
        pandas_dataframe.to_gbq(
            f"{self.schema}.{self.table_name}",
            if_exists=if_exists,
            chunksize=chunksize,
            project_id=self.hook.project_id,
        )

    def pandas_populate_normalize_config(self, ndjson_normalize_sep):
        """
        Validate pandas json_normalize() parameter for databases, since default params result in
        invalid column name. Default parameter result in the columns name containing '.' char.

        :param ndjson_normalize_sep: separator used to normalize nested ndjson.
            https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
        :type ndjson_normalize_sep: str
        :return: return updated config
        :rtype: `dict`
        """
        normalize_config = super().pandas_populate_normalize_config(
            ndjson_normalize_sep
        )
        replacement = "_"
        illegal_char = "."

        meta_prefix = ndjson_normalize_sep
        if meta_prefix and meta_prefix == illegal_char:
            normalize_config["meta_prefix"] = replacement

        record_prefix = normalize_config.get("record_prefix")
        if record_prefix and record_prefix == illegal_char:
            normalize_config["record_prefix"] = replacement

        sep = normalize_config.get("sep")
        if sep is None or sep == illegal_char:
            normalize_config["sep"] = replacement

        return normalize_config
