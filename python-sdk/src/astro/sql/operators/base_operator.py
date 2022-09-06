from abc import ABC

from airflow.models.baseoperator import BaseOperator

from astro.sql.operators.upstream_task_mixin import UpstreamTaskMixin


class AstroSQLBaseOperator(UpstreamTaskMixin, BaseOperator, ABC):
    """Base class for any SQL operator that allows users to define upstream tasks using an `upstream_tasks` parameter"""
