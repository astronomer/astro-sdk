from airflow.decorators.base import get_unique_task_id
from airflow.models import DAG
from airflow.models.baseoperator import BaseOperator


class CleanupOperator(BaseOperator):
    """
    Clean up temporary tables at the end of a DAG run
    :param tables_to_cleanup: List of tbles to drop at the end of the DAG run
    :param task_id: Optional custom task id
    """

    def __init__(
        self,
        *,
        task_id: str = "",
        **kwargs,
    ):
        task_id = task_id or get_unique_task_id("_cleanup")

        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: dict):
        dag: DAG = context["dag"]
        tasks = [t for t in dag.tasks if t.task_id != self.task_id]
        for task in tasks:
            print(
                f"the output of task {task.task_id} is {task.output.resolve(context)}"
            )
