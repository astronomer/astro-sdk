from unittest import mock

import pandas as pd
import pendulum
import pytest
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from openlineage.airflow.extractors import Extractors
from openlineage.airflow.extractors.base import DefaultExtractor
from openlineage.client.facet import DataQualityMetricsInputDatasetFacet, OutputStatisticsOutputDatasetFacet
from openlineage.client.run import Dataset as OpenlineageDataset

from astro import sql as aql
from astro.constants import FileType
from astro.files import File
from astro.lineage.facets import InputFileDatasetFacet, InputFileFacet, OutputDatabaseDatasetFacet
from astro.settings import LOAD_FILE_ENABLE_NATIVE_FALLBACK
from astro.sql import AppendOperator, DataframeOperator, MergeOperator
from astro.sql.operators.export_to_file import ExportToFileOperator
from astro.sql.operators.load_file import LoadFileOperator
from astro.sql.operators.transform import TransformOperator
from astro.table import Metadata, Table
from tests.utils.airflow import create_context

TEST_FILE_LOCATION = "gs://astro-sdk/workspace/sample_pattern.csv"
TEST_TABLE = "test-table"
TEST_INPUT_DATASET_NAMESPACE = "gs://astro-sdk"
TEST_INPUT_DATASET_NAME = "/workspace/sample_pattern"
TEST_OUTPUT_DATASET_NAMESPACE = "bigquery"
TEST_OUTPUT_DATASET_NAME = "astronomer-dag-authoring.astro.test-extractor"
INPUT_STATS_FOR_EXPORT_FILE = [
    OpenlineageDataset(
        namespace=TEST_INPUT_DATASET_NAMESPACE,
        name=TEST_INPUT_DATASET_NAME,
        facets={
            "dataQualityMetrics": DataQualityMetricsInputDatasetFacet(rowCount=117, columnMetrics={}),
        },
    )
]

OUTPUT_STATS_FOR_EXPORT_FILE = [
    OpenlineageDataset(
        namespace=TEST_OUTPUT_DATASET_NAMESPACE,
        name=TEST_OUTPUT_DATASET_NAME,
        facets={
            "outputStatistics": OutputStatisticsOutputDatasetFacet(rowCount=117, size=65),
        },
    )
]
INPUT_STATS = [
    OpenlineageDataset(
        namespace=TEST_INPUT_DATASET_NAMESPACE,
        name=TEST_INPUT_DATASET_NAME,
        facets={
            "input_file_facet": InputFileDatasetFacet(
                number_of_files=1,
                description=None,
                is_pattern=False,
                files=[
                    InputFileFacet(
                        filepath="gs://astro-sdk/workspace/sample_pattern.csv",
                        file_size=65,
                        file_type=FileType.CSV,
                    )
                ],
            )
        },
    )
]

OUTPUT_STATS = [
    OpenlineageDataset(
        namespace=TEST_OUTPUT_DATASET_NAMESPACE,
        name=TEST_OUTPUT_DATASET_NAME,
        facets={
            "output_database_facet": OutputDatabaseDatasetFacet(
                metadata=Metadata(schema="astro", database=None),
                columns=[],
                schema="astro",
                used_native_path=False,
                enabled_native_fallback=LOAD_FILE_ENABLE_NATIVE_FALLBACK,
                native_support_arguments={},
                description=None,
            )
        },
    )
]


@pytest.mark.integration
@mock.patch("airflow.models.TaskInstance.xcom_pull")
def test_python_sdk_load_file_extract_on_complete(mock_xcom_pull):
    """
    Tests that  the custom PythonSDKExtractor is able to process the
    operator's metadata that needs to be extracted as per OpenLineage
    for LoadFileOperator.
    """
    task_id = "load_file"
    load_file_operator = LoadFileOperator(
        task_id=task_id,
        input_file=File(
            TEST_FILE_LOCATION,
            conn_id="bigquery",
            filetype=FileType.CSV,
        ),
        output_table=Table(
            conn_id="postgres_conn",
            name="test_extractor",
        ),
        use_native_support=False,
    )
    mock_xcom_pull.return_value = "postgres_conn"
    load_file_operator.execute(context=create_context(load_file_operator))
    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    task_instance = TaskInstance(task=load_file_operator, run_id=execution_date)
    python_sdk_extractor = Extractors().get_extractor_class(LoadFileOperator)
    assert python_sdk_extractor is DefaultExtractor

    task_meta_extract = python_sdk_extractor(load_file_operator).extract()
    assert task_meta_extract is None

    task_meta = python_sdk_extractor(load_file_operator).extract_on_complete(task_instance=task_instance)
    assert task_meta.name == f"adhoc_airflow.{task_id}"
    assert task_meta.inputs[0].facets["input_file_facet"] == INPUT_STATS[0].facets["input_file_facet"]
    assert task_meta.job_facets == {}
    assert task_meta.run_facets == {}


@pytest.mark.integration
def test_python_sdk_export_file_extract_on_complete():
    """
    Tests that  the custom PythonSDKExtractor is able to process the
    operator's metadata that needs to be extracted as per OpenLineage
    for ExportToFileOperator.
    """
    load_file = LoadFileOperator(
        task_id="load_file",
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
        ),
        output_table=Table(conn_id="sqlite_conn", name="test_extractor"),
    )
    load_file.execute(context=create_context(load_file))

    task_id = "export_file"
    export_file_operator = ExportToFileOperator(
        task_id=task_id,
        input_data=Table(conn_id="sqlite_conn", name="test_extractor"),
        output_file=File(
            path="gs://astro-sdk/workspace/openlineage_export_file.csv",
            conn_id="bigquery",
            filetype=FileType.CSV,
        ),
        if_exists="replace",
    )

    task_instance = TaskInstance(task=export_file_operator)
    python_sdk_extractor = Extractors().get_extractor_class(ExportToFileOperator)
    assert python_sdk_extractor is DefaultExtractor
    task_meta_extract = python_sdk_extractor(export_file_operator).extract()
    assert task_meta_extract is None
    task_meta = python_sdk_extractor(export_file_operator).extract_on_complete(task_instance=task_instance)
    assert task_meta.name == f"adhoc_airflow.{task_id}"
    assert (
        task_meta.inputs[0].facets["dataQualityMetrics"]
        == INPUT_STATS_FOR_EXPORT_FILE[0].facets["dataQualityMetrics"]
    )
    assert (
        task_meta.outputs[0].facets["outputStatistics"]
        == OUTPUT_STATS_FOR_EXPORT_FILE[0].facets["outputStatistics"]
    )
    assert task_meta.job_facets == {}
    assert task_meta.run_facets == {}


@pytest.mark.integration
def test_append_op_extract_on_complete():
    """
    Test extractor ``extract_on_complete`` get called and collect lineage for append operator
    """
    task_id = "append_table"

    src_table_operator = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern.csv", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn"),
    )
    src_table = src_table_operator.execute(context=create_context(src_table_operator))

    target_table_operator = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern.csv", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn"),
    )
    target_table = target_table_operator.execute(context=create_context(target_table_operator))

    op = AppendOperator(
        source_table=src_table,
        target_table=target_table,
    )

    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    task_instance = TaskInstance(task=op, run_id=execution_date)
    python_sdk_extractor = Extractors().get_extractor_class(AppendOperator)
    assert python_sdk_extractor is DefaultExtractor
    task_meta_extract = python_sdk_extractor(op).extract()
    assert task_meta_extract is None
    task_meta = python_sdk_extractor(op).extract_on_complete(task_instance=task_instance)
    assert task_meta.name == f"adhoc_airflow.{task_id}"
    assert task_meta.inputs[0].name == f"astronomer-dag-authoring.astronomer-dag-authoring.{src_table.name}"
    assert task_meta.inputs[0].namespace == "bigquery"
    assert task_meta.inputs[0].facets is not None
    assert len(task_meta.job_facets) > 0
    assert task_meta.run_facets == {}


@pytest.mark.integration
def test_merge_op_extract_on_complete():
    """
    Test extractor ``extract_on_complete`` get called and collect lineage for merge operator
    """
    task_id = "merge"
    src_table_operator = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern.csv", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema="astro")),
    )
    src_table = src_table_operator.execute(context=create_context(src_table_operator))

    target_table_operator = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern.csv", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema="astro")),
    )
    target_table = target_table_operator.execute(context=create_context(target_table_operator))
    op = MergeOperator(
        source_table=src_table,
        target_table=target_table,
        target_conflict_columns=["id"],
        columns=["id", "name"],
        if_conflicts="update",
    )
    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    task_instance = TaskInstance(task=op, run_id=execution_date)

    python_sdk_extractor = Extractors().get_extractor_class(MergeOperator)
    assert python_sdk_extractor is DefaultExtractor
    task_meta_extract = python_sdk_extractor(op).extract()
    assert task_meta_extract is None
    task_meta = python_sdk_extractor(op).extract_on_complete(task_instance=task_instance)

    assert task_meta.name == f"adhoc_airflow.{task_id}"
    assert task_meta.inputs[0].name == f"astronomer-dag-authoring.astro.{src_table.name}"
    assert task_meta.inputs[0].namespace == "bigquery"
    assert task_meta.inputs[0].facets is not None
    assert len(task_meta.job_facets) > 0
    assert task_meta.run_facets == {}


@pytest.mark.integration
def test_python_sdk_transform_extract_on_complete():
    """
    Tests that  the custom PythonSDKExtractor is able to process the
    operator's metadata that needs to be extracted as per OpenLineage
    for TransformOperator.
    """
    load_file = LoadFileOperator(
        task_id="load_file",
        input_file=File(
            path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
        ),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema="astro")),
    )
    imdb_table = load_file.execute(context=create_context(load_file))

    output_table = Table(name="test_name", conn_id="gcp_conn", metadata=Metadata(schema="astro"))
    task_id = "top_five_animations"

    @aql.transform
    def top_five_animations(input_table: Table) -> str:
        return "SELECT title, rating FROM {{ input_table }} LIMIT 5;"

    task = top_five_animations(input_table=imdb_table, output_table=output_table)

    task.operator.execute(context=create_context(task.operator))
    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    task_instance = TaskInstance(task=task.operator, run_id=execution_date)

    python_sdk_extractor = Extractors().get_extractor_class(TransformOperator)
    assert python_sdk_extractor is DefaultExtractor
    task_meta_extract = python_sdk_extractor(task.operator).extract()
    assert task_meta_extract is None
    task_meta = python_sdk_extractor(task.operator).extract_on_complete(task_instance=task_instance)
    assert task_meta.name == f"adhoc_airflow.{task_id}"
    source_code = task_meta.job_facets.get("sourceCode")
    # check for transform code return is present in source code facet.
    validate_string = """return "SELECT title, rating FROM {{ input_table }} LIMIT 5;"""
    assert validate_string in source_code.source
    assert len(task_meta.job_facets) > 0
    assert task_meta.run_facets == {}


@pytest.mark.integration
def test_python_sdk_dataframe_op_extract_on_complete():
    """
    Tests that  the custom PythonSDKExtractor is able to process the
    operator's metadata that needs to be extracted as per OpenLineage
    for DataframeOperator.
    """

    @aql.dataframe(columns_names_capitalization="original")
    def aggregate_data(df: pd.DataFrame):
        new_df = df
        new_df.columns = new_df.columns.str.lower()
        return new_df

    test_list = [["a", "b", "c"], ["AA", "BB", "CC"]]
    dfList = pd.DataFrame(test_list, columns=["COL_A", "COL_B", "COL_C"])
    test_tbl_name = "test_tbl"
    test_schema_name = "test_schema"
    test_db_name = "test_db"

    task = (
        aggregate_data(
            dfList,
            output_table=Table(
                name=test_tbl_name,
                metadata=Metadata(
                    schema=test_schema_name,
                    database=test_db_name,
                ),
                conn_id="sqlite_default",
            ),
        ),
    )

    task[0].operator.execute(context=create_context(task[0].operator))

    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    task_instance = TaskInstance(task=task[0].operator, run_id=execution_date)
    python_sdk_extractor = Extractors().get_extractor_class(DataframeOperator)
    assert python_sdk_extractor is DefaultExtractor
    task_meta_extract = python_sdk_extractor(task[0].operator).extract()
    assert task_meta_extract is None
    task_meta = python_sdk_extractor(task[0].operator).extract_on_complete(task_instance=task_instance)
    assert task_meta.name == "adhoc_airflow.aggregate_data"
    assert task_meta.outputs[0].facets["schema"].fields[0].name == test_schema_name
    assert task_meta.outputs[0].facets["schema"].fields[0].type == test_db_name
    assert task_meta.outputs[0].facets["dataSource"].name == test_tbl_name
    assert task_meta.outputs[0].facets["outputStatistics"].rowCount == len(test_list)
    assert task_meta.run_facets == {}
