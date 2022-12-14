import os
import pathlib

import pandas as pd
import pytest

import astro.sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import Database
from astro.files import File

# Import Operator
from astro.sql.operators.export_table_to_file import export_table_to_file
from astro.table import Table

from ..operators import utils as test_utils

CWD = pathlib.Path(__file__).parent


def test_save_dataframe_to_local(sample_dag):
    @aql.dataframe
    def make_df():
        return pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

    with sample_dag:
        df = make_df()
        aql.export_table_to_file(
            input_data=df,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    df = pd.read_csv("/tmp/saved_df.csv")
    assert df.equals(pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}))


@pytest.mark.parametrize("database_table_fixture", [{"database": Database.SQLITE}], indirect=True)
def test_save_temp_table_to_local(sample_dag, database_table_fixture):
    _, test_table = database_table_fixture
    data_path = str(CWD) + "/../../data/homes.csv"
    with sample_dag:
        table = aql.load_file(input_file=File(path=data_path), output_table=test_table)
        aql.export_file(
            input_data=table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
    test_utils.run_dag(sample_dag)

    output_df = pd.read_csv("/tmp/saved_df.csv")
    input_df = pd.read_csv(data_path)
    assert input_df.equals(output_df)


@pytest.mark.parametrize("database_table_fixture", [{"database": Database.SQLITE}], indirect=True)
def test_save_returns_output_file(sample_dag, database_table_fixture):
    _, test_table = database_table_fixture

    @aql.dataframe
    def validate(df: pd.DataFrame):
        assert not df.empty

    data_path = str(CWD) + "/../../data/homes.csv"
    with sample_dag:
        table = aql.load_file(input_file=File(path=data_path), output_table=test_table)
        file = aql.export_table_to_file(
            input_data=table,
            output_file=File(path="/tmp/saved_df.csv"),
            if_exists="replace",
        )
        res_df = aql.load_file(input_file=file)
        validate(res_df)
    test_utils.run_dag(sample_dag)

    output_df = pd.read_csv("/tmp/saved_df.csv")
    input_df = pd.read_csv(data_path)
    assert input_df.equals(output_df)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
            "file": File(path=str(CWD) + "/../../data/homes.csv"),
        },
    ],
    indirect=True,
)
def test_unique_task_id_for_same_path(
    sample_dag,
    database_table_fixture,
):
    _, test_table = database_table_fixture
    file_name = f"{test_utils.get_table_name('output')}.csv"
    OUTPUT_FILE_PATH = str(CWD) + f"/../../data/{file_name}"

    tasks = []
    with sample_dag:
        for i in range(4):
            params = {
                "input_data": test_table,
                "output_file": File(path=OUTPUT_FILE_PATH),
                "if_exists": "replace",
            }

            if i == 3:
                params["task_id"] = "task_id"
            task = export_table_to_file(**params)
            tasks.append(task)
    test_utils.run_dag(sample_dag)

    assert tasks[0].operator.task_id != tasks[1].operator.task_id
    assert tasks[0].operator.task_id == "export_table_to_file"
    assert tasks[1].operator.task_id == "export_table_to_file__1"
    assert tasks[2].operator.task_id == "export_table_to_file__2"
    assert tasks[3].operator.task_id == "task_id"

    os.remove(OUTPUT_FILE_PATH)


@pytest.mark.skipif(not DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_supported_ds():
    """Test Datasets are set as inlets and outlets"""
    input_data = Table("test_name")
    output_file = File("gs://bucket/object.csv")
    task = aql.export_table_to_file(
        input_data=input_data,
        output_file=output_file,
    )
    assert task.operator.inlets == [input_data]
    assert task.operator.outlets == [output_file]


@pytest.mark.skipif(DATASET_SUPPORT, reason="Inlets/Outlets will only be added for Airflow >= 2.4")
def test_inlets_outlets_non_supported_ds():
    """Test inlets and outlets are not set if Datasets are not supported"""
    input_data = Table("test_name")
    output_file = File("gs://bucket/object.csv")
    task = aql.export_table_to_file(
        input_data=input_data,
        output_file=output_file,
    )
    assert task.operator.inlets == []
    assert task.operator.outlets == []
