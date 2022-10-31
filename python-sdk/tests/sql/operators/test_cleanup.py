import os
import pathlib
from unittest import mock

import airflow
import pandas
import pytest
from airflow import DAG, AirflowException
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.jobs.backfill_job import BackfillJob
from airflow.jobs.scheduler_job import SchedulerJob
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.settings import Session
from airflow.utils.state import State
from airflow.utils.timezone import datetime

import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES, Database
from astro.files import File
from astro.sql.operators.cleanup import CleanupOperator
from astro.sql.operators.load_file import LoadFileOperator
from astro.table import Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent

DEFAULT_FILEPATH = str(pathlib.Path(CWD.parent.parent, "data/sample.csv").absolute())
SUPPORTED_DATABASES_OBJECTS = [
    {
        "database": database,
    }
    for database in Database
]
SUPPORTED_DATABASES_OBJECTS_WITH_FILE = [
    {
        "database": database,
        "file": File(DEFAULT_FILEPATH),
    }
    for database in Database
]


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
def test_cleanup_one_table(database_table_fixture):
    db, test_table = database_table_fixture
    assert db.table_exists(test_table)
    a = aql.cleanup([test_table])
    a.execute({})
    assert not db.table_exists(test_table)


@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {
                    "table": Table(name="non_temp_table"),
                    "file": File(DEFAULT_FILEPATH),
                },
                {
                    "table": Table(),
                    "file": File(DEFAULT_FILEPATH),
                },
            ]
        }
    ],
    indirect=True,
    ids=["named_table"],
)
def test_cleanup_non_temp_table(database_table_fixture, multiple_tables_fixture):
    db, _ = database_table_fixture
    test_table, test_temp_table = multiple_tables_fixture
    assert db.table_exists(test_table)
    assert db.table_exists(test_temp_table)
    test_table.conn_id = db.conn_id
    test_temp_table.conn_id = db.conn_id
    a = aql.cleanup([test_table, test_temp_table])
    a.execute({})
    assert db.table_exists(test_table)
    assert not db.table_exists(test_temp_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
def test_cleanup_non_table(database_table_fixture):
    db, test_table = database_table_fixture
    df = pandas.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    a = aql.cleanup([test_table, df])
    a.execute({})
    assert not db.table_exists(test_table)


@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {
                    "file": File(DEFAULT_FILEPATH),
                },
                {
                    "file": File(DEFAULT_FILEPATH),
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables"],
)
def test_cleanup_multiple_table(database_table_fixture, multiple_tables_fixture):
    db, _ = database_table_fixture
    test_table_1, test_table_2 = multiple_tables_fixture
    assert db.table_exists(test_table_1)
    assert db.table_exists(test_table_2)

    df = pandas.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    a = aql.cleanup([test_table_1, test_table_2, df])
    a.execute({})
    assert not db.table_exists(test_table_1)
    assert not db.table_exists(test_table_2)


@pytest.mark.parametrize(
    "database_table_fixture",
    SUPPORTED_DATABASES_OBJECTS_WITH_FILE,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
@pytest.mark.parametrize(
    "multiple_tables_fixture",
    [
        {
            "items": [
                {
                    "file": File(DEFAULT_FILEPATH),
                },
                {
                    "file": File(DEFAULT_FILEPATH),
                },
            ]
        }
    ],
    indirect=True,
    ids=["two_tables"],
)
def test_cleanup_default_all_tables(sample_dag, database_table_fixture, multiple_tables_fixture):
    @aql.transform()
    def foo(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    db, _ = database_table_fixture
    table_1, table_2 = multiple_tables_fixture
    assert db.table_exists(table_1)
    assert db.table_exists(table_2)

    with sample_dag:
        foo(table_1, output_table=table_2)

        aql.cleanup()
    test_utils.run_dag(sample_dag)

    assert not db.table_exists(table_2)


@pytest.mark.skipif(airflow.__version__ < "2.3.0", reason="Require Airflow version >= 2.3.0")
@pytest.mark.parametrize(
    "database_temp_table_fixture",
    SUPPORTED_DATABASES_OBJECTS,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
def test_cleanup_mapped_task(sample_dag, database_temp_table_fixture):
    db, temp_table = database_temp_table_fixture

    with sample_dag:
        load_file_mapped = LoadFileOperator.partial(task_id="load_file_mapped").expand_kwargs(
            [
                {
                    "input_file": File(path=(CWD.parent.parent / "data/sample.csv").as_posix()),
                    "output_table": temp_table,
                }
            ]
        )

        aql.cleanup(upstream_tasks=[load_file_mapped])
    test_utils.run_dag(sample_dag)

    assert not db.table_exists(temp_table)


@pytest.mark.skipif(airflow.__version__ < "2.3.0", reason="Require Airflow version >= 2.3.0")
@pytest.mark.parametrize(
    "database_temp_table_fixture",
    SUPPORTED_DATABASES_OBJECTS,
    indirect=True,
    ids=SUPPORTED_DATABASES,
)
def test_cleanup_default_all_tables_mapped_task(sample_dag, database_temp_table_fixture):
    db, temp_table = database_temp_table_fixture

    with sample_dag:
        LoadFileOperator.partial(task_id="load_file_mapped").expand_kwargs(
            [
                {
                    "input_file": File(path=(CWD.parent.parent / "data/sample.csv").as_posix()),
                    "output_table": temp_table,
                }
            ]
        )

        aql.cleanup()
    test_utils.run_dag(sample_dag)

    assert not db.table_exists(temp_table)


def test_is_dag_running():
    cleanup_op = CleanupOperator(task_id="cleanup")

    task_instances = []
    for i in range(4):
        op = BashOperator(task_id=f"foo_task_{i}", bash_command="")
        ti = TaskInstance(task=op, state=State.SUCCESS)
        task_instances.append(ti)
    assert not cleanup_op._is_dag_running(task_instances=task_instances)
    task_instances[0].state = State.RUNNING
    assert cleanup_op._is_dag_running(task_instances=task_instances)


@pytest.mark.parametrize("single_worker_mode", [True, False])
@mock.patch("astro.sql.operators.cleanup.CleanupOperator._is_single_worker_mode")
@mock.patch("astro.sql.operators.cleanup.CleanupOperator._is_dag_running")
@mock.patch("astro.sql.operators.cleanup.CleanupOperator.get_all_task_outputs")
def test_error_raised_with_blocking_op_executors(
    mock_get_all_task_outputs,
    mock_is_dag_running,
    mock_is_single_worker_mode,
    single_worker_mode,
):
    """
    Test that when single_worker_mode is used (SequentialExecutor or DebugExecutor) an
    error is raised if the other tasks are still running when cleanup is ran.

    This is because Cleanup will block the entire thread and will cause deadlock until
    execution_timeout of a task is reached
    """
    mock_get_all_task_outputs.return_value = []
    mock_is_dag_running.side_effect = [True, False]
    mock_is_single_worker_mode.return_value = single_worker_mode

    dag = DAG("test_error_raised_with_blocking_op_executors", start_date=datetime(2022, 1, 1))
    cleanup_task = CleanupOperator(dag=dag)
    dr = DagRun(dag_id=dag.dag_id)

    if single_worker_mode:
        with pytest.raises(AirflowException) as exec_info:
            cleanup_task.execute({"dag_run": dr})
        assert exec_info.value.args[0] == (
            "When using a synchronous executor (e.g. SequentialExecutor and DebugExecutor), "
            "the first run of this task will fail on purpose, "
            "so the single worker thread is unblocked to execute other tasks. "
            "The task is set up for retry and eventually works."
        )
    else:
        cleanup_task.execute({"dag_run": dr})


@pytest.mark.parametrize(
    "executor_in_job,executor_in_cfg,expected_val",
    [
        (SequentialExecutor(), "LocalExecutor", True),
        (LocalExecutor(), "LocalExecutor", False),
        (None, "LocalExecutor", False),
        (None, "SequentialExecutor", True),
    ],
)
def test_single_worker_mode_backfill(executor_in_job, executor_in_cfg, expected_val):
    """Test that if we run Backfill Job it should be marked as single worker node"""
    dag = DAG("test_single_worker_mode_backfill", start_date=datetime(2022, 1, 1))
    dr = DagRun(dag_id=dag.dag_id)

    with mock.patch.dict(os.environ, {"AIRFLOW__CORE__EXECUTOR": executor_in_cfg}):
        job = BackfillJob(dag=dag, executor=executor_in_job)
        session = Session()
        session.add(job)
        session.flush()

        dr.creating_job_id = job.id
        assert CleanupOperator._is_single_worker_mode(dr) == expected_val

        session.rollback()


@pytest.mark.parametrize(
    "executor_in_cfg,expected_val",
    [
        ("LocalExecutor", False),
        ("SequentialExecutor", True),
        ("CeleryExecutor", False),
    ],
)
def test_single_worker_mode_scheduler_job(executor_in_cfg, expected_val):
    """Test that if we run Scheduler Job it should be marked as single worker node"""
    dag = DAG("test_single_worker_mode_scheduler_job", start_date=datetime(2022, 1, 1))
    dr = DagRun(dag_id=dag.dag_id)

    with mock.patch.dict(os.environ, {"AIRFLOW__CORE__EXECUTOR": executor_in_cfg}):
        # Scheduler Job in Airflow sets executor from airflow.cfg
        job = SchedulerJob()
        session = Session()
        session.add(job)
        session.flush()

        dr.creating_job_id = job.id
        assert CleanupOperator._is_single_worker_mode(dr) == expected_val

        session.rollback()
