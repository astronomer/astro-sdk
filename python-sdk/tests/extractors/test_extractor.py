import pytest
from airflow.models.taskinstance import TaskInstance
from openlineage.client.run import Dataset as OpenlineageDataset

from astro.constants import FileType
from astro.extractors.extractor import PythonSDKExtractor
from astro.files import File
from astro.sql.operators.load_file import LoadFileOperator
from astro.table import Metadata, Table

TEST_FILE_LOCATION = "gs://astro-sdk/workspace/sample_pattern"
TEST_TABLE = "test-table"
TEST_INPUT_DATASET_NAMESPACE = "gs://astro-sdk"
TEST_INPUT_DATASET_NAME = "/workspace/sample_pattern"
TEST_OUTPUT_DATASET_NAMESPACE = "bigquery://None"
TEST_OUTPUT_DATASET_NAME = "astro.test-extractor"
INPUT_STATS = [
    OpenlineageDataset(
        namespace=TEST_INPUT_DATASET_NAMESPACE,
        name=TEST_INPUT_DATASET_NAME,
        facets={
            "file_size": -1,
            "is_pattern": True,
            "files": ["gs://astro-sdk/workspace/sample_pattern.csv"],
            "number_of_files": 1,
        },
    )
]


OUTPUT_STATS = [
    OpenlineageDataset(
        namespace=TEST_OUTPUT_DATASET_NAMESPACE,
        name=TEST_OUTPUT_DATASET_NAME,
        facets={"metadata": Metadata(schema="astro", database=None), "columns": [], "schema": "astro"},
    )
]


@pytest.mark.integration
def test_python_sdk_load_file_extract_on_complete():
    """
    Tests that  the custom PythonSDKExtractor is able to process the
    operator's metadata that needs to be extracted as per OpenLineage.
    """
    task_id = "load_file"
    load_file_operator = LoadFileOperator(
        task_id=task_id,
        input_file=File(
            "gs://astro-sdk/workspace/sample_pattern",
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
