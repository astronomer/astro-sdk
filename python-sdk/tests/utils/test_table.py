import json
from unittest import mock

import attr
import pytest
from airflow.utils.module_loading import import_string

from astro.constants import Database
from astro.files import File
from astro.sql import LoadFileOperator
from astro.sql.operators.transform import TransformOperator
from astro.table import BaseTable, Table, Metadata
from astro.utils.table import find_first_table

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore

try:
    from kubernetes.client import models as k8s
except ImportError:
    k8s = None

@pytest.mark.parametrize(
    "kwargs,return_type",
    [
        (
            {
                "op_args": (),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {},
            },
            type(None),
        ),
        (
            {
                "op_args": (Table(),),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {"table": Table()},
                "python_callable": lambda table: table,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {"table": Table()},
            },
            BaseTable,
        ),
    ],
    ids=["none", "op_args", "op_kwargs", "parameters"],
)
def test_find_first_table(kwargs, return_type):
    assert isinstance(find_first_table(context={}, **kwargs), return_type)


@pytest.mark.parametrize(
    "kwargs,return_type",
    [
        (
            {
                "op_args": (TransformOperator(python_callable=lambda: "select 1").output,),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {"table": TransformOperator(python_callable=lambda: "select 1").output},
                "python_callable": lambda table: table,
                "parameters": {},
            },
            BaseTable,
        ),
        (
            {
                "op_args": (),
                "op_kwargs": {},
                "python_callable": None,
                "parameters": {"table": TransformOperator(python_callable=lambda: "select 1").output},
            },
            BaseTable,
        ),
    ],
    ids=["op_args", "op_kwargs", "parameters"],
)
@mock.patch("airflow.models.xcom_arg.PlainXComArg.resolve", return_value=Table())
def test_find_first_table_with_xcom_arg(xcom_arg_resolve, kwargs, return_type):
    assert isinstance(find_first_table(context={}, **kwargs), return_type)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
        },
        {
            "database": Database.BIGQUERY,
        },
        {
            "database": Database.POSTGRES,
        },
        {
            "database": Database.SQLITE,
        },
        {
            "database": Database.REDSHIFT,
        },
    ],
    indirect=True,
    ids=["snowflake", "bigquery", "postgresql", "sqlite", "redshift"],
)
def test_row_count(database_table_fixture):
    """
    Load file in bigquery and test the row count of bigquery table
    """
    _, test_table = database_table_fixture
    imdb_table = LoadFileOperator(
        task_id="load_file",
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
        ),
        output_table=test_table,
    ).execute({})

    assert imdb_table.row_count == 117

class XComEncoder(json.JSONEncoder):
    """This encoder allows serializes any object that has attr."""

    def default(self, o: object) -> dict:
        from airflow.serialization.serialized_objects import BaseSerialization

        if attr.has(o.__class__):
            classname = o.__module__ + '.' + o.__class__.__name__

            version = getattr(o, "version", 0)
            # Only include attributes which we can pass back to the classes constructor
            data = attr.asdict(o, recurse=True, filter=lambda a, v: a.init)  # type: ignore[arg-type]
            return {
                "__classname": classname,
                "__version": version,
                "__source": None,
                "__var": BaseSerialization.serialize(data),
            }
        else:
            return super().default(o)


class XComDecoder(json.JSONDecoder):
    """
    This decoder deserializes dicts to objects if they contain
    the `__classname__` key otherwise it will return the dict
    as is.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, dct) -> object:
        if '__classname' in dct:
            from airflow.serialization.serialized_objects import BaseSerialization

            cls = import_string(dct['__classname'])

            version = getattr(cls, "version", 0)
            if '__version' in dct and int(dct['__version']) < version:
                raise TypeError(
                    "serialized version of %s is newer than module version (%s > %s)",
                    dct['__classname'],
                    dct['__version'],
                    version,
                )

            return cls(**BaseSerialization.deserialize(dct['__var']))

        return dct
def test_serde():

    table = Table(name="foo", conn_id="bex", metadata=Metadata(schema="bar", database="baz"))
    serialized = XComEncoder().encode(table)
    print("\n\n\n")
    print(serialized)
    print("\n\n\n")
    t = XComDecoder().decode(serialized)
    print(t)