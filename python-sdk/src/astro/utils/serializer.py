from __future__ import annotations

from typing import Any

from astro.constants import FileType
from astro.files import File
from astro.sql.table import Metadata, Table, TempTable


def serialize(obj: Table | File | Any) -> dict | Any:
    if isinstance(obj, Table) or isinstance(obj, TempTable):
        return {
            "class": "Table",
            "name": obj.name,
            "metadata": {
                "schema": obj.metadata.schema,
                "database": obj.metadata.database,
            },
            "temp": obj.temp,
            "conn_id": obj.conn_id,
        }
    elif isinstance(obj, File):
        filetype = None if not obj.filetype else obj.filetype.value
        return {
            "class": "File",
            "conn_id": obj.conn_id,
            "path": obj.path,
            "filetype": filetype,
            "normalize_config": obj.normalize_config,
        }
    else:
        return obj


def deserialize(obj: dict) -> Table | File | Any:
    if (
        not isinstance(obj, dict)
        or not obj.get("class")
        or obj["class"] not in ["Table", "File"]
    ):
        return obj
    if obj["class"] == "Table":
        return Table(
            name=obj["name"],
            metadata=Metadata(**obj["metadata"]),
            temp=obj["temp"],
            conn_id=obj["conn_id"],
        )
    elif obj["class"] == "File":
        return File(
            conn_id=obj["conn_id"],
            path=obj["path"],
            filetype=FileType(obj["filetype"]),
            normalize_config=obj["normalize_config"],
        )
    return obj
