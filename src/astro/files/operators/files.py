from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

from astro.files.locations import create_file_location


class _ListFileOperator(BaseOperator):
    """List the file available at path and storage

    :param task_id: The task id for uniquely identify a task in a DAG
    :param path: A path pattern for which you want to get a list of file
    :param conn_id: connection id for the services
    """

    def __init__(self, task_id: str, path: str, conn_id: str, **kwargs):

        super().__init__(task_id=task_id, **kwargs)
        self.path = path
        self.conn_id = conn_id

    def execute(self, context: Context) -> Any:  # skipcq: PYL-W0613
        location = create_file_location(self.path, self.conn_id)
        return location.paths
