import pendulum
import pytest
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from openlineage.client.run import Dataset as OpenlineageDataset

from astro import sql as aql
from astro.constants import FileType
from astro.files import File
from astro.lineage.extractor import PythonSDKExtractor
from astro.lineage.facets import InputFileDatasetFacet, InputFileFacet, OutputDatabaseDatasetFacet
from astro.sql import AppendOperator, MergeOperator, TransformOperator
from astro.sql.operators.load_file import LoadFileOperator
from astro.table import Metadata, Table
from tests.utils.airflow import create_context

TEST_FILE_LOCATION = "gs://astro-sdk/workspace/sample_pattern"
TEST_TABLE = "test-table"
TEST_INPUT_DATASET_NAMESPACE = "gs://astro-sdk"
TEST_INPUT_DATASET_NAME = "/workspace/sample_pattern"
TEST_OUTPUT_DATASET_NAMESPACE = "bigquery"
TEST_OUTPUT_DATASET_NAME = "astronomer-dag-authoring.astro.test-extractor"
INPUT_STATS = [
    OpenlineageDataset(
        namespace=TEST_INPUT_DATASET_NAMESPACE,
        name=TEST_INPUT_DATASET_NAME,
        facets={
            "input_file_facet": InputFileDatasetFacet(
                number_of_files=1,
                description=None,
                is_pattern=True,
                files=[
                    InputFileFacet(
                        filepath="gs://astro-sdk/workspace/sample_pattern.csv",
                        file_size=-1,
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
                enabled_native_fallback=True,
                native_support_arguments={},
                description=None,
            )
        },
    )
]


@pytest.mark.integration
def test_python_sdk_load_file_extract_on_complete():
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
        output_table=Table(conn_id="bigquery", name="test-extractor", metadata=Metadata(schema="astro")),
        use_native_support=False,
    )

    task_instance = TaskInstance(task=load_file_operator)

    python_sdk_extractor = PythonSDKExtractor(load_file_operator)
    task_meta_extract = python_sdk_extractor.extract()
    assert task_meta_extract is None

    task_meta = python_sdk_extractor.extract_on_complete(task_instance)
    assert task_meta.name == f"adhoc_airflow.{task_id}"
    assert task_meta.inputs[0] == INPUT_STATS[0]
    assert task_meta.outputs[0] == OUTPUT_STATS[0]
    assert task_meta.job_facets == {}
    assert task_meta.run_facets == {}


@pytest.mark.integration
def test_append_op_extract_on_complete():
    """
    Test extractor ``extract_on_complete`` get called and collect lineage for append operator
    """
    task_id = "append_table"

    src_table = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn"),
    ).execute({})

    target_table = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn"),
    ).execute({})

    op = AppendOperator(
        source_table=src_table,
        target_table=target_table,
    )

    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    task_instance = TaskInstance(task=op, run_id=execution_date)

    python_sdk_extractor = PythonSDKExtractor(op)
    task_meta_extract = python_sdk_extractor.extract()
    assert task_meta_extract is None

    task_meta = python_sdk_extractor.extract_on_complete(task_instance)
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
    src_table = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema="astro")),
    ).execute({})

    target_table = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="gs://astro-sdk/workspace/sample_pattern", filetype=FileType.CSV),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema="astro")),
    ).execute({})
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

    python_sdk_extractor = PythonSDKExtractor(op)
    task_meta_extract = python_sdk_extractor.extract()
    assert task_meta_extract is None

    task_meta = python_sdk_extractor.extract_on_complete(task_instance)
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
    imdb_table = LoadFileOperator(
        task_id="load_file",
        input_file=File(path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
        output_table=Table(conn_id="gcp_conn", metadata=Metadata(schema="astro")),
    ).execute({})

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

    python_sdk_extractor = PythonSDKExtractor(task.operator)
    assert type(python_sdk_extractor.get_operator_classnames()) is list
    task_meta = python_sdk_extractor.extract_on_complete(task_instance)
    assert task_meta.name == f"adhoc_airflow.{task_id}"
    assert task_meta.outputs[0].facets["stats"].size is None
    assert len(task_meta.job_facets) > 0
    assert task_meta.run_facets == {}
