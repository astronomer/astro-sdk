from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.models import BaseOperator
from airflow.utils.context import Context

from astro.files.base import File
from astro.files.locations import create_file_location


class ListFileOperator(BaseOperator):
    """List the file available at path and storage

    :param task_id: The task id for uniquely identify a task in a DAG
    :param path: A path pattern for which you want to get a list of file
    :param conn_id: connection id for the services
    """

    template_fields = ("path", "conn_id")

    def __init__(self, path: str, conn_id: str, task_id: str = "", **kwargs):
        task_id = task_id or get_unique_task_id(
            "get_file_list", dag=kwargs.get("dag"), task_group=kwargs.get("task_group")
        )
        self.path = path
        self.conn_id = conn_id
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Context) -> Any:  # skipcq: PYL-W0613
        location = create_file_location(self.path, self.conn_id)
        # Get list of files excluding folders
        return [
            File(path=path, conn_id=location.conn_id)
            for path in location.paths
            if not path.endswith("/")
        ]
