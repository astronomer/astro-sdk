import os
import pathlib
from unittest import mock

import astro.sql as aql
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
from astro.constants import Database
from astro.files import File
from astro.sql.operators.cleanup import CleanupOperator
from astro.sql.table import Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent

DEFAULT_FILEPATH = str(pathlib.Path(CWD.parent.parent, "data/sample.csv").absolute())
SQLITE_ONLY = [
    {"database": Database.SQLITE, "file": File(DEFAULT_FILEPATH)},
]
SUPPORTED_DATABASES = [
    {
        "database": Database.SQLITE,
    },
    {
        "database": Database.POSTGRES,
    },
    {
        "database": Database.BIGQUERY,
    },
    {
        "database": Database.SNOWFLAKE,
    },
]
SUPPORTED_DATABASES_WITH_FILE = [
    dict(x, **{"file": File(DEFAULT_FILEPATH)}) for x in SUPPORTED_DATABASES
]


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    SQLITE_ONLY,
    indirect=True,
    ids=["sqlite"],
)
def test_cleanup_one_table(database_table_fixture):
    db, test_table = database_table_fixture
    assert db.table_exists(test_table)
    a = aql.cleanup([test_table])
    a.execute({})
    assert not db.table_exists(test_table)


@pytest.mark.parametrize(
    "database_table_fixture",
    SQLITE_ONLY,
    indirect=True,
    ids=["sqlite"],
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
    SQLITE_ONLY,
    indirect=True,
    ids=["sqlite"],
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
    SQLITE_ONLY,
    indirect=True,
    ids=["sqlite"],
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
    SQLITE_ONLY,
    indirect=True,
    ids=["sqlite"],
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
def test_cleanup_default_all_tables(
    sample_dag, database_table_fixture, multiple_tables_fixture
):
    @aql.transform
    def foo(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    table_1, table_2 = multiple_tables_fixture
    with sample_dag:
        foo(table_1)
        foo(table_2)

        aql.cleanup([])
    test_utils.run_dag(sample_dag)


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

    dag = DAG(
        "test_error_raised_with_blocking_op_executors", start_date=datetime(2022, 1, 1)
    )
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
