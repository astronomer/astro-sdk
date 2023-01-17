from __future__ import annotations

from typing import Any

from airflow.decorators.base import get_unique_task_id
from airflow.models import BaseOperator

from astro.files.base import File
from astro.files.locations import create_file_location
from astro.utils.compat.typing import Context


class ListFileOperator(BaseOperator):
    """
    List the file available at path and storage

    :param task_id: The task id for uniquely identify a task in a DAG
    :param path: A path pattern for which you want to get a list of file
    :param conn_id: Airflow connection id.
        This will be used to identify the right Airflow hook at runtime to connect with storage services
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
        files = get_file_list_func(self.path, self.conn_id)
        # Get list of files excluding folders
        return [File(path=file, conn_id=self.conn_id) for file in files]


def get_file_list_func(path: str, conn_id: str) -> list[str]:
    """Function to get list of files from a bucket"""
    location = create_file_location(path, conn_id)
    # Get list of files excluding folders
    return [path for path in location.paths if not path.endswith("/")]
