from __future__ import annotations

import json
import pickle
from typing import Any

from astro.files import File
from astro.sql.table import Table, TempTable


def serialize(obj: Table | File | Any) -> dict | Any:
    if isinstance(obj, Table) or isinstance(obj, TempTable):
        return obj.to_json()
    elif isinstance(obj, File):
        return obj.to_json()
    elif isinstance(obj, list):
        return [serialize(o) for o in obj]
    elif isinstance(obj, str):
        return {"class": "string", "value": obj}
    else:
        return _attempt_to_serialize_unknown_object(obj)


def _attempt_to_serialize_unknown_object(obj: object):
    try:
        return json.dumps(obj)
    except Exception:
        return pickle.dumps(obj).hex()


def deserialize(obj: dict | str | list) -> Table | File | Any:
    if isinstance(obj, list) or isinstance(obj, tuple):
        return [deserialize(o) for o in obj]
    if (
        isinstance(obj, dict)
        and obj.get("class")
        and obj["class"] in ["Table", "File", "string"]
    ):
        if obj["class"] == "Table":
            return Table.from_json(obj)
        elif obj["class"] == "Table":
            return File.from_json(obj)
        else:
            return obj["value"]
    else:
        return _attempt_to_deser_unknown_object(obj)


def _attempt_to_deser_unknown_object(obj: dict | str):
    try:
        return json.loads(obj)
    except Exception:
        try:
            return pickle.loads(bytes.fromhex(obj))
        except Exception:
            return obj
