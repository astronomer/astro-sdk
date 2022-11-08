from __future__ import annotations

from attr import define
from airflow.providers.databricks.hooks.databricks import DatabricksHook

from astro.table import BaseTable, Metadata
from databricks_cli.sdk.api_client import ApiClient

@define(slots=False)
class DeltaTable(BaseTable):
    spark_configs = {}
    conn_id = ""

    def __init__(self, conn_id, **kwargs):
        self.conn_id = conn_id
        super().__init__(conn_id=conn_id, **kwargs)

    def hook(self):
        return DatabricksHook(databricks_conn_id=self.conn_id)

    def api_client(self):
        conn = DatabricksHook(databricks_conn_id=self.conn_id).get_conn()
        api_client = ApiClient(host=conn.host, token=conn.extra_dejson['token'])
        return api_client

    def to_json(self):
        return {
            "class": "DeltaTable",
            "name": self.name,
            "metadata": {
                "schema": self.metadata.schema,
                "database": self.metadata.database,
            },
            "temp": self.temp,
            "conn_id": self.conn_id,
        }

    @classmethod
    def from_json(cls, obj: dict):
        return DeltaTable(
            name=obj["name"],
            metadata=Metadata(**obj["metadata"]),
            temp=obj["temp"],
            conn_id=obj["conn_id"],
        )
