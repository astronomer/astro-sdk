import time
from typing import Any, List

from airflow.decorators.base import get_unique_task_id
from airflow.models.baseoperator import BaseOperator
from airflow.utils.state import State

from astro.databases import create_database
from astro.sql.operators.base import BaseSQLOperator
from astro.sql.operators.dataframe import DataframeOperator
from astro.sql.table import Table


def get_expected_task_outputs(tasks, context):
    return [
        task.output.resolve(context)
        for task in tasks
        # for the moment, these are the only two classes that create temporary tables.
        # Users can extend BaseSQLOperator if they want their classes caught by this
        if isinstance(task, BaseSQLOperator) or isinstance(task, DataframeOperator)
    ]


def filter_for_temp_tables(task_outputs: List[Any]):
    return [t for t in task_outputs if isinstance(t, Table) and t.temp]


class CleanupOperator(BaseOperator):
    """
    Clean up temporary tables at the end of a DAG run.

    By default if no tables are
    :param tables_to_cleanup: List of tbles to drop at the end of the DAG run
    :param task_id: Optional custom task id
    :param run_sync_mode: Whether to wait for the DAG to finish or not. Set to False if you want to immediately
    clean all DAGs. Not that if you supply anything int `tables_to_cleanup` this argument is ignored.
    """

    template_fields = ("tables_to_cleanup",)

    def __init__(
        self,
        *,
        tables_to_cleanup: List[Table] = [],
        task_id: str = "",
        run_sync_mode: bool = False,
        **kwargs,
    ):
        self.tables_to_cleanup = tables_to_cleanup
        self.run_sync_mode = run_sync_mode
        task_id = task_id or get_unique_task_id("_cleanup")

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: dict):
        if not self.tables_to_cleanup:
            # tables not provided, attempt to either immediately run or wait for all other tasks to finish
            if not self.run_sync_mode:
                self.wait_for_dag_to_finish(context)
            self.tables_to_cleanup = self.get_all_task_outputs(context=context)
        temp_tables = filter_for_temp_tables(self.tables_to_cleanup)
        for table in temp_tables:
            self.drop_table(table)

    def drop_table(self, table):
        db = create_database(table.conn_id)
        self.log.info("Dropping table %s", table.name)
        db.drop_table(table)

    def _is_dag_running(self, task_instances):
        """
        Given a list of task instances, determine whether the DAG (minus the current cleanup task) is still
        running.

        :param task_instances:
        :return:
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

    def wait_for_dag_to_finish(self, context):
        """
        In the event that we are not given any tables, we will want to wait for all other tasks to finish before
        we delete temporary tables. This prevents a scenario where either a) we delete temporary tables that
        are still in use, or b) we run this function too early and then there are temporary tables that don't get
        deleted.

        Eventually this function should be made into an asynchronous function s.t. this operator does not take up a
        worker slot.
        :param context:
        :return:
        """

        dag_is_running = True
        current_dagrun = context["dag_run"]
        while dag_is_running:
            dag_is_running = self._is_dag_running(current_dagrun.get_task_instances())
            if dag_is_running:
                time.sleep(5)

    def get_all_task_outputs(self, context):
        """
        In the scenario where we are not given a list of tasks to follow, we will want to gather all temporary tables
        To prevent scenarios where we grab objects that are not tables, we try to only follow up on SQL operators or
        the dataframe operator, as these are the operators that return temporary tables.

        :param context:
        :return:
        """
        self.log.info("No tables provided, will delete all temporary tables")
        tasks = [t for t in self.dag.tasks if t.task_id != self.task_id]
        task_outputs = get_expected_task_outputs(tasks=tasks, context=context)
        return task_outputs
