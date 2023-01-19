import datetime
import shutil
import tempfile
from pathlib import Path

import airflow
import pytest
from conftest import DEFAULT_DATE
from freezegun import freeze_time

from sql_cli.dag_generator import Workflow, generate_dag
from sql_cli.exceptions import DagCycle, EmptyDag, WorkflowFilesDirectoryNotFound

CWD = Path(__file__).parent


def test_workflow(workflow, workflow_with_parameters, sql_file, sql_file_with_parameters):
    """Test that a simple build will return the sql files."""
    assert workflow.sorted_workflow_files() == [sql_file]
    assert workflow_with_parameters.sorted_workflow_files() == [sql_file_with_parameters]


def test_workflow_with_cycle(workflow_with_cycle):
    """Test that an exception is being raised when it is not a DAG."""
    with pytest.raises(DagCycle):
        assert workflow_with_cycle.sorted_workflow_files()


def test_workflow_without_workflow_files():
    """Test that an exception is being raised when it does not have any workflow files."""
    with pytest.raises(EmptyDag):
        Workflow(dag_id="workflow_without_workflow_files", start_date=DEFAULT_DATE, workflow_files=[])


sample_workflow_with_dataset_schedule = b"""
workflow:
  schedule:
    - dataset:
        uri: s3://some-dataset/upstream1.parquet
    - dataset:
        uri: s3://some-dataset/upstream2.parquet
"""


def test_workflow_from_yaml_with_dataset_as_schedule():
    """Test that we are able to generate a workflow class from a YAML file."""
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(sample_workflow_with_dataset_schedule)
        tmp.flush()
        workflow = Workflow.from_yaml(Path(tmp.name), dag_id="default", workflow_files=[Path("/tmp")])
        expected = [
            {"dataset": {"uri": "s3://some-dataset/upstream1.parquet"}},
            {"dataset": {"uri": "s3://some-dataset/upstream2.parquet"}},
        ]

        assert workflow.schedule == expected


@freeze_time(datetime.datetime(1984, 5, 11, 14, 0, 0))
def test_workflow_from_yaml_defaults():
    """Test that we are able to generate a workflow class from a YAML file."""
    with tempfile.NamedTemporaryFile() as tmp:
        tmp.write(b"workflow:\n  some_key: some_value")
        tmp.flush()
        workflow = Workflow.from_yaml(Path(tmp.name), dag_id="default", workflow_files=[Path("/tmp")])
        assert not workflow.catchup
        assert workflow.dag_id == "default"
        assert workflow.description is None
        assert workflow.end_date is None
        assert not workflow.is_paused_upon_creation
        assert workflow.orientation == "LR"
        assert workflow.schedule == "@daily"
        assert workflow.start_date == "1984-05-10T14:00:00"
        assert workflow.tags == ["SQL"]
        assert workflow.workflow_files


def test_workflow_from_yaml_file_overrides_defaults():
    """Test that we are able to generate a workflow class from a YAML file."""
    workflow_yaml = CWD / "workflows/example_deploy/workflow.yaml"
    workflow = Workflow.from_yaml(workflow_yaml, dag_id="test", workflow_files=[Path("/tmp")])
    assert workflow.catchup
    assert workflow.dag_id == "test"
    assert workflow.description == "Sample workflow used to load data from a CSV file into a SQLite database"
    assert workflow.end_date is None
    assert not workflow.is_paused_upon_creation
    assert workflow.orientation == "LR"
    assert workflow.schedule == "@hourly"
    assert workflow.start_date == "2015-12-1"
    assert workflow.tags == ["SQLite"]
    assert workflow.workflow_files


@pytest.mark.parametrize("generate_tasks", [True, False])
def test_generate_dag(root_directory, dags_directory, generate_tasks):
    """Test that the whole DAG generation process including sql files parsing works."""
    dag_file = generate_dag(
        directory=root_directory, dags_directory=dags_directory, generate_tasks=generate_tasks
    )
    assert dag_file


@pytest.mark.parametrize("generate_tasks", [True, False])
def test_generate_dag_invalid_directory(root_directory, dags_directory, generate_tasks):
    """Test that an exception is being raised when the directory does not exist."""
    with pytest.raises(WorkflowFilesDirectoryNotFound):
        generate_dag(directory=Path("foo"), dags_directory=dags_directory, generate_tasks=generate_tasks)


compliant_dag = """# NOTE: This is an auto-generated file. Please do not edit this file manually.
from pathlib import Path

import airflow
from airflow.utils import timezone
from astro import sql as aql
from astro.constants import FileType
from astro.files import File
from astro.table import Metadata, Table

DAGS_FOLDER = Path(__file__).parent.as_posix()


with airflow.DAG(
    dag_id="basic",
    start_date=timezone.parse("2016-12-21T03:49:00"),
    catchup=False,
    is_paused_upon_creation=False,
    orientation="LR",
    tags=[
        "SQL",
    ],
    schedule="@daily",
) as dag:
    a = aql.transform_file(
        file_path=f"{DAGS_FOLDER}/include/basic/a.sql",
        parameters={},
        conn_id="my_test_sqlite",
        op_kwargs={
            "output_table": Table(
                name="a",
            ),
        },
        task_id="a",
    )
    b = aql.transform_file(
        file_path=f"{DAGS_FOLDER}/include/basic/b.sql",
        parameters={
            "a": a,
        },
        op_kwargs={
            "output_table": Table(
                name="b",
            ),
        },
        task_id="b",
    )
    c = aql.transform_file(
        file_path=f"{DAGS_FOLDER}/include/basic/c.sql",
        parameters={
            "a": a,
            "b": b,
        },
        op_kwargs={
            "output_table": Table(
                name="c",
            ),
        },
        task_id="c",
    )
"""


@freeze_time(datetime.datetime(2016, 12, 22, 3, 49, 0))
def test_generate_dag_black_compliant(root_directory, dags_directory):
    dag_file = generate_dag(directory=root_directory, dags_directory=dags_directory, generate_tasks=True)
    assert dag_file.read_text() == compliant_dag


@freeze_time(datetime.datetime(2023, 1, 16, 17, 0, 0))
def test_generate_dag_using_workflow_yaml(tmp_path):
    root_directory = CWD / "workflows/example_deploy/"
    dag_file = generate_dag(directory=root_directory, dags_directory=tmp_path, generate_tasks=True)
    content = dag_file.read_text()
    expected = """
with airflow.DAG(
    dag_id="example_deploy",
    start_date=timezone.parse("2015-12-1"),
    catchup=True,
    description="Sample workflow used to load data from a CSV file into a SQLite database",
    is_paused_upon_creation=False,
    orientation="LR",
    tags=[
        "SQLite",
    ],
    schedule="@hourly",
) as dag:
"""
    assert expected in content


@pytest.mark.skipif(
    airflow.__version__ < "2.4.0", reason="Require airflow.Dataset available in version >= 2.4.0"
)
def test_generate_dag_using_workflow_yaml_with_dataset_as_schedule(tmp_path):
    """Test that we are able to generate a workflow class from a YAML file."""
    with open(tmp_path / "workflow.yml", "wb") as workflow_config:
        workflow_config.write(sample_workflow_with_dataset_schedule)
        workflow_config.flush()
        shutil.copy(CWD / "workflows/example_deploy/original_imdb_movies.yaml", tmp_path / "load_task.yml")
        dag_file = generate_dag(directory=tmp_path, dags_directory=tmp_path, generate_tasks=True)
        content = dag_file.read_text()
        expected = """
    schedule=[
        airflow.Dataset(uri="s3://some-dataset/upstream1.parquet"),
        airflow.Dataset(uri="s3://some-dataset/upstream2.parquet"),
    ],"""
        assert expected in content
