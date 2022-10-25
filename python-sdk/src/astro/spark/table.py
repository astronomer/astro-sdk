from __future__ import annotations
from astro.table import BaseTable
from attr import define, field, fields_dict
from astro.table import Metadata

@define(slots=False)
class DeltaTable(BaseTable):
    spark_configs={}
    path=""
    conn_id=""

    def __init__(self, path, conn_id, **kwargs):
        self.path = path
        self.conn_id = conn_id
        super().__init__(conn_id=conn_id, **kwargs)

    def to_json(self):
        return {
            "class": "DeltaTable",
            "name": self.name,
            "path": self.path,
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
            path=obj["path"],
            metadata=Metadata(**obj["metadata"]),
            temp=obj["temp"],
            conn_id=obj["conn_id"],
        )

