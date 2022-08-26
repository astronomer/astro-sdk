from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.models import BaseOperator
from airflow.utils.context import Context

from astro.files.base import File
from astro.files.locations import create_file_location


class ListFileOperator(BaseOperator):
    """List the file available at path and storage

    :param task_id: The task id for uniquely identify a task in a DAG
    :param file: File object a reference to file path pattern and Airflow connection ID
    """

    def __init__(self, file: File, task_id: str = "", **kwargs):
        task_id = task_id or get_unique_task_id(
            "get_file_list", dag=kwargs.get("dag"), task_group=kwargs.get("task_group")
        )
        self.file = file
        super().__init__(task_id=task_id, **kwargs)

    def execute(self, context: Context) -> Any:  # skipcq: PYL-W0613
        location = create_file_location(self.file.path, self.file.conn_id)
        # Get list of files excluding folders
        return [
            File(path=path, conn_id=location.conn_id)
            for path in location.paths
            if not path.endswith("/")
        ]
