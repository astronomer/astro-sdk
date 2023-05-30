import os
import pathlib
from unittest import mock

import pytest
from airflow import DAG, AirflowException, __version__ as airflow_version
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.sequential_executor import SequentialExecutor
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.settings import Session
from airflow.utils.state import State
from airflow.utils.timezone import datetime
from packaging import version

from astro.constants import Database
from astro.files import File
from astro.sql.operators.cleanup import CleanupOperator

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


def test_has_task_failed():
    cleanup_op = CleanupOperator(task_id="cleanup")

    task_instances = []
    for i in range(4):
        op = BashOperator(task_id=f"foo_task_{i}", bash_command="")
        ti = TaskInstance(task=op, state=State.SUCCESS)
        task_instances.append(ti)
    assert not cleanup_op._has_task_failed(task_instances=task_instances)
    task_instances[0].state = State.FAILED
    assert cleanup_op._has_task_failed(task_instances=task_instances)


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


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.6.0"),
    reason="BackfillJobRunner and Job classes are only available in airflow >= 2.6",
)
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
    from airflow.jobs.backfill_job_runner import BackfillJobRunner
    from airflow.jobs.job import Job

    dag = DAG("test_single_worker_mode_backfill", start_date=datetime(2022, 1, 1))
    dr = DagRun(dag_id=dag.dag_id)

    with mock.patch.dict(os.environ, {"AIRFLOW__CORE__EXECUTOR": executor_in_cfg}):
        job = Job(executor=executor_in_job)
        session = Session()
        session.add(job)
        session.flush()
        BackfillJobRunner(job=job, dag=dag)

        dr.creating_job_id = job.id
        assert CleanupOperator._is_single_worker_mode(dr) == expected_val

        session.rollback()


@pytest.mark.skipif(
    version.parse(airflow_version) >= version.parse("2.6.0"),
    reason="BackfillJob class is not available in airflow < 2.6",
)
@pytest.mark.parametrize(
    "executor_in_job,executor_in_cfg,expected_val",
    [
        (SequentialExecutor(), "LocalExecutor", True),
        (LocalExecutor(), "LocalExecutor", False),
        (None, "LocalExecutor", False),
        (None, "SequentialExecutor", True),
    ],
)
def test_single_worker_mode_backfill_airflow_2_5(executor_in_job, executor_in_cfg, expected_val):
    """Test that if we run Backfill Job it should be marked as single worker node"""
    from airflow.jobs.backfill_job import BackfillJob

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


@pytest.mark.skipif(
    version.parse(airflow_version) < version.parse("2.6.0"),
    reason="SchedulerJobRunner and Job classes are only available in airflow >= 2.6.0",
)
@pytest.mark.parametrize(
    "executor_in_job,executor_in_cfg,expected_val",
    [
        (SequentialExecutor(), "LocalExecutor", True),
        (LocalExecutor(), "LocalExecutor", False),
        (None, "LocalExecutor", False),
        (None, "SequentialExecutor", True),
    ],
)
def test_single_worker_mode_scheduler_job(executor_in_job, executor_in_cfg, expected_val):
    """Test that if we run Scheduler Job it should be marked as single worker node"""
    from airflow.jobs.job import Job
    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner

    dag = DAG("test_single_worker_mode_scheduler_job", start_date=datetime(2022, 1, 1))
    dr = DagRun(dag_id=dag.dag_id)

    with mock.patch.dict(os.environ, {"AIRFLOW__CORE__EXECUTOR": executor_in_cfg}):
        # Scheduler Job in Airflow sets executor from airflow.cfg
        job = Job(executor=executor_in_job)
        session = Session()
        session.add(job)
        session.flush()
        SchedulerJobRunner(job=job)

        dr.creating_job_id = job.id
        assert CleanupOperator._is_single_worker_mode(dr) == expected_val

        session.rollback()


@pytest.mark.skipif(
    version.parse(airflow_version) >= version.parse("2.6.0"),
    reason="SchedulerJob class is not available in airflow < 2.6",
)
@pytest.mark.parametrize(
    "executor_in_job,expected_val",
    [
        ("LocalExecutor", False),
        ("SequentialExecutor", True),
        ("CeleryExecutor", False),
    ],
)
def test_single_worker_mode_scheduler_job_airflow_2_5(executor_in_job, expected_val):
    """Test that if we run Scheduler Job it should be marked as single worker node"""
    from airflow.jobs.scheduler_job import SchedulerJob

    dag = DAG("test_single_worker_mode_scheduler_job", start_date=datetime(2022, 1, 1))
    dr = DagRun(dag_id=dag.dag_id)

    with mock.patch.dict(os.environ, {"AIRFLOW__CORE__EXECUTOR": executor_in_job}):
        # Scheduler Job in Airflow sets executor from airflow.cfg
        job = SchedulerJob()
        session = Session()
        session.add(job)
        session.flush()

        dr.creating_job_id = job.id
        assert CleanupOperator._is_single_worker_mode(dr) == expected_val

        session.rollback()
