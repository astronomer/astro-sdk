from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context

from astro.files.locations import create_file_location


class _ListFileOperator(BaseOperator):

    def __init__(self, task_id: str, path: str, conn_id: str, **kwargs):

        super().__init__(task_id=task_id, **kwargs)
        self.path = path
        self.conn_id = conn_id

    def execute(self, context: Context) -> Any:
        location = create_file_location(self.path, self.conn_id)
        return location.paths
