from abc import ABC

from airflow.models.baseoperator import BaseOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from astro.sql.operators.upstream_task_mixin import UpstreamTaskMixin


class AstroSQLBaseOperator(LoggingMixin, UpstreamTaskMixin, BaseOperator, ABC):
    """Base class for any SQL operator that allows users to define upstream tasks using an `upstream_tasks` parameter"""
