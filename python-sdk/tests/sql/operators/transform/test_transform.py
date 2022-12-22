import pathlib

import pytest

from astro import sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.table import Table

cwd = pathlib.Path(__file__).parent


@pytest.mark.skipif(not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    imdb_table = (Table(name="imdb", conn_id="sqlite_default"),)
    output_table = Table(name="test_name")

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return "SELECT title, rating FROM {{ input_table }} LIMIT 5;"

    task = top_five_animations(input_table=imdb_table, output_table=output_table)
    assert task.operator.outlets == [output_table]


@pytest.mark.skipif(DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    imdb_table = (Table(name="imdb", conn_id="sqlite_default"),)
    output_table = Table(name="test_name")

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return "SELECT title, rating FROM {{ input_table }} LIMIT 5;"

    task = top_five_animations(input_table=imdb_table, output_table=output_table)
    assert task.operator.outlets == []
