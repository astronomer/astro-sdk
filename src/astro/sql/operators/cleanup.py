import os
import time
from typing import Any, List, Optional

from airflow.decorators.base import get_unique_task_id
from airflow.exceptions import AirflowException
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from airflow.utils.state import State

from astro.databases import create_database
from astro.sql.operators.base import BaseSQLOperator
from astro.sql.operators.dataframe import DataframeOperator
from astro.sql.operators.load_file import LoadFile
from astro.sql.table import Table


def resolve_tables_from_tasks(
    tasks: List[BaseOperator], context: Context
) -> List[Table]:
    """
    For the moment, these are the only two classes that create temporary tables.
    This function allows us to only resolve xcom for those objects (to reduce how much data is brought into the worker).

    We also process these values one at a time so the system can garbage collect non-table objects (otherwise we might
    run into a situation where we pull in a bunch of dataframes and overwhelm the worker).
    :param tasks: A list of operators from airflow that we can resolve
    :param context: Context of the DAGRun so we can resolve against the XCOM table
    :return:
    """
    res = []
    for task in tasks:
        if isinstance(task, (DataframeOperator, BaseSQLOperator, LoadFile)):
            t = task.output.resolve(context)
            if isinstance(t, Table):
                res.append(t)
    return res


def filter_for_temp_tables(task_outputs: List[Any]) -> List[Table]:
    return [t for t in task_outputs if isinstance(t, Table) and t.temp]


class CleanupOperator(BaseOperator):
    """
    Clean up temporary tables at the end of a DAG run.

    By default if no tables are placed, the task will wait for all other tasks to run before deleting all temporary
    tables (WARNING: DO NOT RUN THIS MODE IF USING THE SEQUENTIAL EXECUTOR. IT WILL CAUSE A BLOCK).
    :param tables_to_cleanup: List of tbles to drop at the end of the DAG run
    :param task_id: Optional custom task id
    :param run_sync_mode: Whether to wait for the DAG to finish or not. Set to False if you want to immediately
    clean all DAGs. Note that if you supply anything to `tables_to_cleanup` this argument is ignored.
    """

    template_fields = ("tables_to_cleanup",)

    def __init__(
        self,
        *,
        tables_to_cleanup: Optional[List[Table]] = None,
        task_id: str = "",
        run_sync_mode: bool = False,
        **kwargs,
    ):
        self.tables_to_cleanup = tables_to_cleanup or []
        self.run_immediately = run_sync_mode
        self.single_worker_mode = os.getenv("AIRFLOW__ASTRO__SINGLE_WORKER_MODE")
        task_id = task_id or get_unique_task_id("_cleanup")

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Context):
        self.log.info("Execute Cleanup")
        if not self.tables_to_cleanup:
            # tables not provided, attempt to either immediately run or wait for all other tasks to finish
            if not self.run_immediately:
                self.wait_for_dag_to_finish(context)
            self.tables_to_cleanup = self.get_all_task_outputs(context=context)
        self.log.info(
            "Tables found for cleanup: %s",
            ",".join([t.name for t in self.tables_to_cleanup]),
        )
        temp_tables = filter_for_temp_tables(self.tables_to_cleanup)
        for table in temp_tables:
            self.drop_table(table)

    def drop_table(self, table: Table):
        db = create_database(table.conn_id)
        self.log.info("Dropping table %s", table.name)
        db.drop_table(table)

    def _is_dag_running(self, task_instances: List[TaskInstance]) -> bool:
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
            and ti.state not in [State.SUCCESS, State.FAILED, State.SKIPPED]
        ]
        if running_tasks:
            self.log.info(
                "waiting on the following tasks to complete before cleaning up: %s",
                running_tasks,
            )
            return True
        else:
            return False

    def wait_for_dag_to_finish(self, context: Context):
        """
        In the event that we are not given any tables, we will want to wait for all other tasks to finish before
        we delete temporary tables. This prevents a scenario where either a) we delete temporary tables that
        are still in use, or b) we run this function too early and then there are temporary tables that don't get
        deleted.

        Eventually this function should be made into an asynchronous function s.t. this operator does not take up a
        worker slot.
        :param context:
        """

        dag_is_running = True
        current_dagrun = context["dag_run"]
        while dag_is_running:
            dag_is_running = self._is_dag_running(current_dagrun.get_task_instances())
            if dag_is_running:
                if self.single_worker_mode:
                    self.log.warning(
                        "You are currently running the 'single worker mode', where the task will fail and retry to"
                        "unblock the single worker thread. This mode should only be used for SequentialExecutor as it"
                        "creates the appearance of a failed task."
                    )
                    raise AirflowException(
                        "You are currently running the 'single worker mode', where the task will fail and retry to"
                        "unblock the single worker thread. This mode should only be used for SequentialExecutor as it"
                        "creates the appearance of a failed task. The task should requeue and retry soon."
                    )
                else:
                    self.log.warning(
                        "You are currently running the 'waiting mode', where the task will wait"
                        "for all other tasks to complete. Please note that for asynchronous executors (e.g. "
                        "sequentialexecutor and debugexecutor, this mode will cause locks."
                    )
                    time.sleep(5)

    def get_all_task_outputs(self, context: Context) -> List[Table]:
        """
        In the scenario where we are not given a list of tasks to follow, we will want to gather all temporary tables
        To prevent scenarios where we grab objects that are not tables, we try to only follow up on SQL operators or
        the dataframe operator, as these are the operators that return temporary tables.

        :param context:
        """
        self.log.info("No tables provided, will delete all temporary tables")
        tasks = [t for t in self.dag.tasks if t.task_id != self.task_id]
        task_outputs = resolve_tables_from_tasks(tasks=tasks, context=context)
        return task_outputs
