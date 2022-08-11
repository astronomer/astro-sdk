from airflow.models.xcom_arg import XComArg

class UpstreamTaskMixin:
    def __init__(self, **kwargs):
        upstream_tasks = kwargs.pop("upstream_tasks", [])

        super().__init__(**kwargs)

        for task in upstream_tasks:
            if isinstance(task, XComArg):
                self.set_upstream(task.operator)