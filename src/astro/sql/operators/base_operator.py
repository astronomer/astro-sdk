from airflow.models.baseoperator import BaseOperator

from astro.sql.operators.upstream_task_mixin import UpstreamTaskMixin


class AstroSQLBaseOperator(UpstreamTaskMixin, BaseOperator):
    pass
