from __future__ import annotations

import time
from datetime import timedelta
from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.utils.state import State
from astro.sql.operators.base_decorator import BaseSQLDecoratedOperator
from astro.sql.operators.dataframe import DataframeOperator
from astro.sql.operators.load_file import LoadFileOperator
from astro.sql.table import Table

from astro.databases import create_database


def filter_for_temp_tables(task_outputs: list[Any]) -> list[Table]:
    return [t for t in task_outputs if isinstance(t, Table) and t.temp]


class CleanupOperator(BaseOperator):
    """
    Clean up temporary tables at the end of a DAG run. Temporary tables are the ones that are
    generated by the SDK (where you do not pass a name arg to Table) or the ones that has the name
    that starts with ``_tmp``.

    By default if no tables are placed, the task will wait for all other tasks to run before deleting
    all temporary tables.

    If using a synchronous executor (e.g. SequentialExecutor and DebugExecutor), this task will initially
    fail on purpose, so the executor is unblocked and can run other tasks. Users may have to define
    custom values for `retries` and `retry_delay` if they intend to use one of these executors.

    :param tables_to_cleanup: List of tables to drop at the end of the DAG run
    :param task_id: Optional custom task id
    :param retries: The number of retries that should be performed before failing the task.
        Very relevant if using a synchronous executor. The default is 3.
    :param retry_delay: Delay between running retries. Very relevant if using a synchronous executor.
        The default is 10s.
    :param run_sync_mode: Whether to wait for the DAG to finish or not. Set to False if you want
        to immediately clean all DAGs. Note that if you supply anything to `tables_to_cleanup`
        this argument is ignored.
    """

    template_fields = ("tables_to_cleanup",)

    def __init__(
        self,
        *,
        tables_to_cleanup: list[Table] | None = None,
        task_id: str = "",
        retries: int = 3,
        retry_delay: timedelta = timedelta(seconds=10),
        run_sync_mode: bool = False,
        **kwargs,
    ):
        self.tables_to_cleanup = tables_to_cleanup or []
        self.run_immediately = run_sync_mode
        task_id = task_id or get_unique_task_id("cleanup")

        super().__init__(
            task_id=task_id, retries=retries, retry_delay=retry_delay, **kwargs
        )

    def execute(self, context: Context) -> None:
        self.log.info("Execute Cleanup")
        if not self.tables_to_cleanup:
            # tables not provided, attempt to either immediately run or wait for all other tasks to finish
            if not self.run_immediately:
                self.wait_for_dag_to_finish(context)
            self.tables_to_cleanup = self.get_all_task_outputs(context=context)
        temp_tables = filter_for_temp_tables(self.tables_to_cleanup)
        self.log.info(
            "Tables found for cleanup: %s",
            ",".join([t.name for t in temp_tables]),
        )
        for table in temp_tables:
            self.drop_table(table)

    def drop_table(self, table: Table) -> None:
        db = create_database(table.conn_id)
        self.log.info("Dropping table %s", table.name)
        db.drop_table(table)

    def _is_dag_running(self, task_instances: list[TaskInstance]) -> bool:
        """
        Given a list of task instances, determine whether the DAG (minus the current cleanup task) is still
        running.

        :param task_instances:
        :return: boolean to show if all tasks besides this one have completed
        """
        running_tasks = [
            (ti.task_id, ti.state)
            for ti in task_instances
            if ti.task_id != self.task_id
            and ti.state
            not in [
                State.SUCCESS,
                State.FAILED,
                State.SKIPPED,
                State.UPSTREAM_FAILED,
                State.REMOVED,
                State.SHUTDOWN,
            ]
        ]
        if running_tasks:
            self.log.info(
                "waiting on the following tasks to complete before cleaning up: %s",
                running_tasks,
            )
            return True
        else:
            return False

    def wait_for_dag_to_finish(self, context: Context) -> None:
        """
        In the event that we are not given any tables, we will want to wait for all other tasks to finish before
        we delete temporary tables. This prevents a scenario where either a) we delete temporary tables that
        are still in use, or b) we run this function too early and then there are temporary tables that don't get
        deleted.

        Eventually this function should be made into an asynchronous function s.t. this operator does not take up a
        worker slot.

        :param context: TI's Context dictionary
        """

        dag_is_running = True
        current_dagrun = context["dag_run"]
        single_worker_mode = self._is_single_worker_mode(current_dagrun)
        while dag_is_running:
            dag_is_running = self._is_dag_running(current_dagrun.get_task_instances())
            if dag_is_running:
                if single_worker_mode:
                    raise AirflowException(
                        "When using a synchronous executor (e.g. SequentialExecutor and DebugExecutor), "
                        "the first run of this task will fail on purpose, "
                        "so the single worker thread is unblocked to execute other tasks. "
                        "The task is set up for retry and eventually works."
                    )
                self.log.warning(
                    "You are currently running the 'waiting mode', where the task will wait "
                    "for all other tasks to complete. Please note that for asynchronous executors (e.g. "
                    "SequentialExecutor and DebugExecutor, this mode will cause locks."
                )
                time.sleep(5)

    @classmethod
    def _is_single_worker_mode(cls, current_dagrun: DagRun) -> bool:
        # This is imported in the function as this import is expensive and is only needed for
        # this function to find the executor
        from airflow.configuration import conf

        job_id = current_dagrun.creating_job_id
        executor: str = conf.get("core", "EXECUTOR")
        if job_id:
            executor = cls._get_executor_from_job_id(job_id) or executor
        return executor in ["SequentialExecutor", "DebugExecutor"]

    @staticmethod
    def _get_executor_from_job_id(job_id: int) -> str | None:
        from airflow.jobs.base_job import BaseJob
        from airflow.utils.session import create_session

        with create_session() as session:
            job = session.get(BaseJob, job_id)
        return job.executor_class if job else None

    def get_all_task_outputs(self, context: Context) -> list[Table]:
        """
        In the scenario where we are not given a list of tasks to follow, we will want to gather all temporary tables
        To prevent scenarios where we grab objects that are not tables, we try to only follow up on SQL operators or
        the dataframe operator, as these are the operators that return temporary tables.

        :param context: Context of the DAGRun so we can resolve against the XCOM table
        """
        self.log.info("No tables provided, will delete all temporary tables")
        tasks = [t for t in self.dag.tasks if t.task_id != self.task_id]
        task_outputs = self.resolve_tables_from_tasks(tasks=tasks, context=context)
        return task_outputs

    def resolve_tables_from_tasks(
        self, tasks: list[BaseOperator], context: Context
    ) -> list[Table]:
        """
        For the moment, these are the only two classes that create temporary tables.
        This function allows us to only resolve xcom for those objects
        (to reduce how much data is brought into the worker).

        We also process these values one at a time so the system can garbage collect non-table objects
        (otherwise we might run into a situation where we pull in a bunch of dataframes and overwhelm the worker).
        :param tasks: A list of operators from airflow that we can resolve
        :param context: Context of the DAGRun so we can resolve against the XCOM table
        :return: List of tables
        """
        res = []
        for task in tasks:
            if isinstance(
                task, (DataframeOperator, BaseSQLDecoratedOperator, LoadFileOperator)
            ):
                try:
                    t = task.output.resolve(context)
                    if isinstance(t, Table):
                        res.append(t)
                except AirflowException:
                    self.log.info(
                        "xcom output for %s not found. Will not clean up this task",
                        task.task_id,
                    )

        return res


def cleanup(tables_to_cleanup: list[Table] | None = None, **kwargs) -> CleanupOperator:
    """
    Clean up temporary tables once either the DAG or upstream tasks are done

    The cleanup operator allows for two possible scenarios: Either a user wants to clean up a specific set of tables
    during the DAG run, or the user wants to ensure that all temporary tables are deleted once the DAG run is finished.
    The idea here is to ensure that even if a user doesn't have access to a "temp" schema, that astro does not leave
    hanging tables once execution is done.

    :param tables_to_cleanup: A list of tables to cleanup, defaults to waiting for all upstream tasks to finish
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    """
    return CleanupOperator(tables_to_cleanup=tables_to_cleanup, **kwargs)
