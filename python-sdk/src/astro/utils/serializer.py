from __future__ import annotations

from typing import Any

import numpy
from sqlalchemy.engine.row import LegacyRow

from astro.files import File
from astro.sql.table import Table, TempTable


def serialize(obj: Table | File | Any) -> dict | Any:
    if isinstance(obj, Table) or isinstance(obj, TempTable):
        obj.to_json()
    elif isinstance(obj, File):
        obj.to_json()
    elif isinstance(obj, numpy.integer):
        return int(obj)
    elif isinstance(obj, numpy.floating):
        return float(obj)
    elif isinstance(obj, numpy.ndarray):
        return obj.tolist()
    elif isinstance(obj, list):
        x = [serialize(o) for o in obj]
        return x
    elif isinstance(obj, LegacyRow):
        val = obj._asdict()
        return {"class": "LegacyRow", "value": val}
    else:
        return obj


def deserialize(obj: dict) -> Table | File | Any:
    if isinstance(obj, list):
        return [deserialize(o) for o in obj]
    if (
        not isinstance(obj, dict)
        or not obj.get("class")
        or obj["class"] not in ["Table", "File", "LegacyRow"]
    ):
        return obj
    if obj["class"] == "Table":
        return Table.from_json(obj)
    elif obj["class"] == "File":
        return File.from_json(obj)
    elif obj["class"] == "LegacyRow":
        return LegacyRow(**obj["value"])
    return obj
