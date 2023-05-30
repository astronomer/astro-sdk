import os
import pathlib
import warnings
from unittest import mock

import pandas as pd
import pytest

from astro import sql as aql
from astro.airflow.datasets import DATASET_SUPPORT
from astro.constants import Database, FileType
from astro.dataframes.load_options import PandasCsvLoadOptions
from astro.files import File
from astro.sql.operators.load_file import LoadFileOperator, load_file
from astro.table import Metadata, Table
from tests.utils.airflow import create_context

from ..operators import utils as test_utils

OUTPUT_TABLE_NAME = test_utils.get_table_name("load_file_test_table")
CWD = pathlib.Path(__file__).parent


def test_unique_task_id_for_same_path(sample_dag):
    tasks = []

    with sample_dag:
        for index in range(4):
            params = {
                "input_file": File(path=str(CWD) + "/../../data/homes.csv"),
                "output_table": Table(conn_id="sqlite_default", metadata=Metadata(database="pagila")),
            }
            if index == 3:
                params["task_id"] = "task_id"

            task = load_file(**params)
            tasks.append(task)

    test_utils.run_dag(sample_dag)

    assert tasks[0].operator.task_id != tasks[1].operator.task_id
    assert tasks[1].operator.task_id == "load_file__1"
    assert tasks[2].operator.task_id == "load_file__2"
    assert tasks[3].operator.task_id == "task_id"


# TODO(kaxil): Change the following test to just check the rendered filename
@mock.patch.dict(os.environ, {"AIRFLOW_VAR_FOO": "templated_file_name"})
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
        },
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_load_file_templated_filename(sample_dag, database_table_fixture):
    db, test_table = database_table_fixture
    with sample_dag:
        load_file(
            input_file=File(path=str(CWD) + "/../../data/{{ var.value.foo }}/example.csv"),
            output_table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = db.export_table_to_pandas_dataframe(test_table)
    assert len(df) == 3


def is_dict_subset(superset: dict, subset: dict) -> bool:
    """
    Compare superset and subset to check if the latter is a subset of former.
    Note: dict1 <= dict2 was not working on multilevel nested dicts.
    """
    for key, val in subset.items():
        print(key, val)
        if isinstance(val, dict):
            if key not in superset:
                return False
            result = is_dict_subset(superset[key], subset[key])
            if not result:
                return False
        elif superset[key] != val:
            return False
    return True


def test_aql_load_file_columns_names_capitalization_dataframe(sample_dag):
    filename = str(CWD.parent) + "/../data/homes_pattern_1.csv"

    from airflow.decorators import task

    @task
    def validate(input_df_1, input_df_2, input_df_3):
        assert isinstance(input_df_1, pd.DataFrame)
        assert isinstance(input_df_2, pd.DataFrame)
        assert isinstance(input_df_3, pd.DataFrame)
        assert all(x.isupper() for x in list(input_df_1.columns))
        assert all(x.islower() for x in list(input_df_2.columns))
        assert all(x.islower() for x in list(input_df_3.columns))

    with sample_dag:
        loaded_df_1 = load_file(
            input_file=File(path=filename, filetype=FileType.CSV),
            columns_names_capitalization="upper",
        )
        loaded_df_2 = load_file(
            input_file=File(path=filename, filetype=FileType.CSV),
            columns_names_capitalization="lower",
        )
        loaded_df_3 = load_file(
            input_file=File(path=filename, filetype=FileType.CSV),
            columns_names_capitalization="original",
        )
        validate(loaded_df_1, loaded_df_2, loaded_df_3)

    test_utils.run_dag(sample_dag)


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


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
        },
    ],
    indirect=True,
    ids=["Sqlite"],
)
@pytest.mark.parametrize("if_exists", ["replace", "append"])
def test_tables_creation_if_they_dont_exist(database_table_fixture, if_exists):
    """
    Verify creation of new tables in case we pass if_exists=replace/append if they don't exists.
    """
    path = str(CWD) + "/../../data/homes_main.csv"
    db, test_table = database_table_fixture
    load_file_task = load_file(input_file=File(path), output_table=test_table, if_exists=if_exists)
    load_file_task.operator.execute(context=create_context(load_file_task.operator))

    database_df = db.export_table_to_pandas_dataframe(test_table)
    assert database_df.shape == (3, 9)


# TODO: Remove this test in astro-sdk 2.0.0
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
        },
    ],
    indirect=True,
    ids=["Sqlite"],
)
@pytest.mark.parametrize("if_exists", ["append"])
def test_load_file_native_support_kwargs_warnings_message(database_table_fixture, if_exists):
    """Assert the warning log for load_options will be replacing native_support_kwargs parameter"""
    path = str(CWD) + "/../../data/homes_main.csv"
    _, test_table = database_table_fixture
    with pytest.warns(
        expected_warning=DeprecationWarning,
        match=r"`load_options` will replace `native_support_kwargs`",
    ):
        load_file(
            input_file=File(path),
            output_table=test_table,
            if_exists=if_exists,
            native_support_kwargs={"dummy": "dummy"},
        )

    with pytest.warns(
        expected_warning=DeprecationWarning,
        match=r"`load_options` will replace `native_support_kwargs`",
    ):
        LoadFileOperator(
            input_file=File(path),
            output_table=test_table,
            if_exists=if_exists,
            native_support_kwargs={"dummy": "dummy"},
        )


def test_deprecation_warning_for_loadoptions():
    path = str(CWD) + "/../../data/homes_main.csv"
    with warnings.catch_warnings(record=True) as warn:
        load_file(
            input_file=File(path),
            output_table=Table(conn_id="sqlite_default"),
            use_native_support=False,
            load_options=PandasCsvLoadOptions(delimiter="$"),
        )
    assert (
        "`PandasCsvLoadOptions` will be replaced by `astro.dataframes.load_options.PandasLoadOptions` in"
        " astro-sdk-python>=2.0.0. Please use `astro.dataframes.load_options.PandasLoadOptions` class instead."
        in [str(w.message) for w in warn]
    )
