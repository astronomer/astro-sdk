from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context

from astro.sql.operators.upstream_task_mixin import UpstreamTaskMixin


class AstroSQLBaseOperator(UpstreamTaskMixin, BaseOperator):
    def execute(self, context: Context) -> Any:
        pass
