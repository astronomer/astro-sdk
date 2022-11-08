from airflow.exceptions import AirflowException

try:
    # Airflow >= 2.3
    from airflow.models.abstractoperator import AbstractOperator
except ImportError:  # pragma: no cover
    # Airflow < 2.3
    from airflow.models.baseoperator import BaseOperator as AbstractOperator

from airflow.models.xcom_arg import XComArg


class UpstreamTaskMixin:
    def __init__(self, **kwargs):
        upstream_tasks = kwargs.pop("upstream_tasks", [])

        super().__init__(**kwargs)

        for task in upstream_tasks:
            if isinstance(task, XComArg):
                self.set_upstream(task.operator)
            elif isinstance(task, AbstractOperator):
                self.set_upstream(task)
            else:
                raise AirflowException(
                    "Cannot upstream a non-task, please only use XcomArg or operators for this" " parameter"
                )
