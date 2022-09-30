from __future__ import annotations

import json
import pickle
from typing import Any

import attrs

from astro.files import File
from astro.sql.table import Table, TempTable
from json import JSONDecodeError
from pickle import UnpicklingError
import logging

log = logging.getLogger("astro.utils.serializer")


def serialize(obj: Table | File | Any) -> dict | Any:
    if isinstance(obj, (Table, TempTable)):
        return attrs.asdict(obj, recurse=True, filter=lambda a, v: a.init)
    elif isinstance(obj, File):
        return attrs.asdict(obj, recurse=True, filter=lambda a, v: a.init)
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


def _is_serialized_astro_object(obj) -> bool:
    return bool(obj.get("class") and obj["class"] in ["Table", "File", "string"])


def deserialize(obj: dict | str | list) -> Table | File | Any:
    if isinstance(obj, (list, tuple)):
        return [deserialize(o) for o in obj]
    if isinstance(obj, dict) and _is_serialized_astro_object(obj):
        if obj["class"] == "Table":
            log.debug("Found table dictionary %s, will attempt to deserialize", obj)
            return Table(**obj)
        elif obj["class"] == "File":
            log.debug("Found file dictionary %s, will attempt to deserialize", obj)
            return _deserialize_file(obj)
        else:
            return obj["value"]
    elif isinstance(obj, str):
        log.debug("Found string, will attempt to deserialize")
        return _attempt_to_deser_unknown_object(obj)
    else:
        return obj


def _deserialize_file(obj):
    file = File(**obj)
    if file.is_dataframe:
        return file.export_to_dataframe()
    return file


def _attempt_to_deser_unknown_object(obj: str):
    try:
        log.debug("Attempting to deserialize object %s into a json object", obj)
        return json.loads(obj)
    except JSONDecodeError:
        try:
            log.debug("Json debugging failed for object %s, attempting to pickle deserialize", obj)
            return pickle.loads(bytes.fromhex(obj))
        except UnpicklingError:
            log.debug("unpickling failed for object %s, returning raw object", obj)
            return obj
