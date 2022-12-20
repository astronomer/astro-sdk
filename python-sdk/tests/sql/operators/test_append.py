from __future__ import annotations

import os
import pathlib
from unittest import mock

import pytest

from astro import sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.files import File
from astro.sql.operators.append import AppendOperator
from astro.table import Metadata, Table
from tests.utils.airflow import create_context

CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "test_columns,expected_columns",
    [
        (["sell", "list"], {"sell": "sell", "list": "list"}),
        (("sell", "list"), {"sell": "sell", "list": "list"}),
        (
            {"s_sell": "t_sell", "s_list": "t_list"},
            {"s_sell": "t_sell", "s_list": "t_list"},
        ),
    ],
)
def test_columns_params(test_columns, expected_columns):
    """
    Test that the columns param in AppendOperator takes list/tuple/dict and converts them to dict
    before sending over to db.append_table()
    """
    source_table = Table(name="source_table", conn_id="test1", metadata=Metadata(schema="test"))
    target_table = Table(name="target_table", conn_id="test2", metadata=Metadata(schema="test"))
    append_task = AppendOperator(
        source_table=source_table,
        target_table=target_table,
        columns=test_columns,
    )
    assert append_task.columns == expected_columns
    with mock.patch("astro.databases.base.BaseDatabase.append_table") as mock_append, mock.patch.dict(
        os.environ,
        {"AIRFLOW_CONN_TEST1": "sqlite://", "AIRFLOW_CONN_TEST2": "sqlite://"},
    ):
        append_task.execute(context=create_context(append_task))
        mock_append.assert_called_once_with(
            source_table=source_table,
            target_table=target_table,
            source_to_target_columns_map=expected_columns,
        )


def test_invalid_columns_param():
    """Test that an error is raised when an invalid columns type is passed"""
    source_table = Table(name="source_table", conn_id="test1", metadata=Metadata(schema="test"))
    target_table = Table(name="target_table", conn_id="test2", metadata=Metadata(schema="test"))
    with pytest.raises(ValueError) as exec_info:
        AppendOperator(
            source_table=source_table,
            target_table=target_table,
            columns={"set_item_1", "set_item_2", "set_item_3"},
        )
    assert (
        exec_info.value.args[0]
        == "columns is not a valid type. Valid types: [tuple, list, dict], Passed: <class 'set'>"
    )


@pytest.mark.skipif(not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    input_file = File("gs://bucket/object.csv")
    output_table = Table("test_name")
    task = aql.load_file(
        input_file=input_file,
        output_table=output_table,
    )
    assert task.operator.inlets == [input_file]
    assert task.operator.outlets == [output_table]


@pytest.mark.skipif(DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    input_file = File("gs://bucket/object.csv")
    output_table = Table("test_name")
    task = aql.load_file(
        input_file=input_file,
        output_table=output_table,
    )
    assert task.operator.inlets == []
    assert task.operator.outlets == []
