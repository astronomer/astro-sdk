from __future__ import annotations

import json
import pickle
from typing import Any

import numpy
from astro.files import File
from astro.sql.table import Table, TempTable


def serialize(obj: Table | File | Any) -> dict | Any:
    if isinstance(obj, Table) or isinstance(obj, TempTable):
        return obj.to_json()
    elif isinstance(obj, File):
        return obj.to_json()
    elif isinstance(obj, numpy.integer):
        return int(obj)
    elif isinstance(obj, numpy.floating):
        return float(obj)
    elif isinstance(obj, numpy.ndarray):
        return obj.tolist()
    elif isinstance(obj, list):
        return [serialize(o) for o in obj]
    else:
        try:
            return json.dumps(obj)
        except Exception:
            return pickle.dumps(obj).hex()


def deserialize(obj: dict) -> Table | File | Any:
    if isinstance(obj, list) or isinstance(obj, tuple):
        return [deserialize(o) for o in obj]
    if (
        isinstance(obj, dict)
        and obj.get("class")
        and obj["class"] in ["Table", "File", "LegacyRow"]
    ):
        if obj["class"] == "Table":
            return Table.from_json(obj)
        elif obj["class"] == "File":
            return File.from_json(obj)
    else:
        try:
            return json.loads(obj)
        except Exception:
            try:
                return pickle.loads(bytes.fromhex(obj))
            except Exception:
                return obj
