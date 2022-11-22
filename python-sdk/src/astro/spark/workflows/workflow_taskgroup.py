from __future__ import annotations

from logging import Logger
from typing import TYPE_CHECKING, Dict, Sequence

from airflow.serialization.enums import DagAttributeTypes

if TYPE_CHECKING:
    pass
import weakref
from typing import Any

from airflow.exceptions import AirflowException, DuplicateTaskIdFound, TaskAlreadyInTaskGroup
from airflow.models.operator import BaseOperator
from airflow.models.taskmixin import DAGNode
from airflow.utils.task_group import TaskGroup, TaskGroupContext
from databricks_cli.dbfs.api import DbfsApi, DbfsPath
from databricks_cli.jobs.api import JobsApi
from databricks_cli.pipelines.api import PipelinesApi
from databricks_cli.runs.api import RunsApi
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.secrets.api import SecretApi

from astro.utils.typing_compat import Context

job_config = {
    "name": "airflow-job-test",
    "email_notifications": {"no_alert_for_skipped_runs": False},
    "timeout_seconds": 0,
    "max_concurrent_runs": 1,
    "tasks": [
        {
            "task_key": "job-task-1",
            "notebook_task": {
                "notebook_path": "/Users/faisal@astronomer.io/job-task-1",
                "source": "WORKSPACE",
            },
            "depends_on": [],
            "job_cluster_key": "workflow-job-cluster",
            "timeout_seconds": 0,
            "email_notifications": {},
        },
        {
            "task_key": "job-task-2",
            "depends_on": [{"task_key": "job-task-1"}],
            "notebook_task": {
                "notebook_path": "/Users/faisal@astronomer.io/job-task-2",
                "source": "WORKSPACE",
            },
            "job_cluster_key": "workflow-job-cluster",
            "timeout_seconds": 0,
            "email_notifications": {},
        },
        {
            "task_key": "job-task-3",
            "depends_on": [{"task_key": "job-task-1"}],
            "notebook_task": {
                "notebook_path": "/Users/faisal@astronomer.io/job-task-3",
                "source": "WORKSPACE",
            },
            "job_cluster_key": "workflow-job-cluster",
            "timeout_seconds": 0,
            "email_notifications": {},
        },
    ],
    "job_clusters": [
        {
            "job_cluster_key": "workflow-job-cluster",
            "new_cluster": {
                "cluster_name": "",
                "spark_version": "10.4.x-scala2.12",
                "spark_conf": {
                    "spark.master": "local[*, 4]",
                    "spark.databricks.cluster.profile": "singleNode",
                },
                "aws_attributes": {
                    "first_on_demand": 1,
                    "availability": "SPOT_WITH_FALLBACK",
                    "zone_id": "us-east-1a",
                    "spot_bid_price_percent": 100,
                },
                "node_type_id": "m5d.large",
                "driver_node_type_id": "m5d.large",
                "custom_tags": {"ResourceClass": "SingleNode"},
                "spark_env_vars": {"PYSPARK_PYTHON": "/databricks/python3/bin/python3"},
                "enable_elastic_disk": True,
                "runtime_engine": "STANDARD",
                "num_workers": 0,
            },
        }
    ],
    "format": "MULTI_TASK",
}


class DatabricksNotebookOperator(BaseOperator):
    def __init__(self, notebook_path: str, source: str, **kwargs):
        self.notebook_path = notebook_path
        self.source = source
        super().__init__(**kwargs)


class CreateDatabricksWorkflowOperator(BaseOperator):
    supported_operators = [DatabricksNotebookOperator]

    def __init__(
        self,
        task_id,
        databricks_conn_id,
        job_cluster_json: dict[str, object] = None,
        tasks_to_convert: list[BaseOperator] = None,
        **kwargs,
    ):
        self.job_cluster_key = job_cluster_json["job_cluster_key"]
        self.job_cluster_json = job_cluster_json
        self.tasks_to_convert = tasks_to_convert or []
        self.relevant_upstreams = [task_id]
        self.databricks_conn_id = databricks_conn_id
        super().__init__(task_id=task_id, **kwargs)

    def add_task(self, task: BaseOperator):
        self.tasks_to_convert.append(task)

    def _generate_task_json(self, task: BaseOperator) -> dict[str, object]:
        result = {
            "task_key": task.task_id,
            "depends_on": list([t for t in task.upstream_task_ids if t in self.relevant_upstreams]),
            "job_cluster_key": self.job_cluster_key,
            "timeout_seconds": 0,
            "email_notifications": {},
        }

        if isinstance(task, DatabricksNotebookOperator):
            result["notebook_task"] = {
                "notebook_path": task.notebook_path,
                "source": task.source,
            }
        return result

    def execute(self, context: Context) -> Any:
        from astro.spark.delta import DeltaDatabase

        task_json = []
        for task in self.tasks_to_convert:
            if not isinstance(task, *self.supported_operators):
                raise AirflowException(
                    f"Operator {task.__class__} not yet supported for databricks workflows."
                    f" please choose one of the following: {','.join([o.__name__ for o in self.supported_operators])}"
                )
            task_json.append(self._generate_task_json(task))
        full_json = {
            "name": "airflow-job-test",
            "email_notifications": {"no_alert_for_skipped_runs": False},
            "timeout_seconds": 0,
            "max_concurrent_runs": 1,
            "tasks": task_json,
            "job_clusters": [self.job_cluster_json],
            "format": "MULTI_TASK",
        }
        import json

        db = DeltaDatabase(conn_id=self.databricks_conn_id)
        api_client = db.api_client()
        jobs_api = JobsApi(api_client=api_client)
        jobs_api.create_job(json=full_json)
        run_id = jobs_api.run_now(
            job_id="airflow-job-test",
            jar_params=None,
            python_params=None,
            notebook_params=None,
            spark_submit_params=None,
        )
        runs_api = RunsApi(api_client)
        import time

        while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "PENDING":
            print("job pending")
            time.sleep(5)

        while runs_api.get_run(run_id)["state"]["life_cycle_state"] == "RUNNING":
            print("job running")
            time.sleep(3)

        print(f"final state: {runs_api.get_run(run_id)['state']['result_state']}")


class DatabricksWorkflowTaskGroup(TaskGroup):
    def __init__(self, job_cluster_json, databricks_conn_id, **kwargs):
        self.databricks_conn_id = databricks_conn_id
        self.job_cluster_json = job_cluster_json
        super().__init__(**kwargs)

    def __exit__(self, _type, _value, _tb):
        roots = self.roots
        create_databricks_workflow_task: CreateDatabricksWorkflowOperator = CreateDatabricksWorkflowOperator(
            dag=self.dag,
            task_id="launch",
            databricks_conn_id=self.databricks_conn_id,
            job_cluster_json=self.job_cluster_json,
        )
        for task in roots:
            create_databricks_workflow_task.set_upstream(task_or_task_list=list(task.upstream_list))

        for task_id, task in self.children.items():
            if task_id != f"{self.group_id}.launch":
                create_databricks_workflow_task.relevant_upstreams.append(task_id)
                create_databricks_workflow_task.add_task(task)

        create_databricks_workflow_task.set_downstream(roots)
        super().__exit__(_type, _value, _tb)
