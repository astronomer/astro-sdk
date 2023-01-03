from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    pass
from typing import Any

from airflow.configuration import conf
from airflow.models.operator import BaseOperator

from astro.databricks.api_utils import create_and_run_job
from astro.utils.typing_compat import Context


class DatabricksNotebookOperator(BaseOperator):
    def __init__(
        self,
        notebook_path: str,
        source: str,
        databricks_conn_id: str,
        existing_cluster_id: str = "",
        new_cluster_spec: dict | None = None,
        **kwargs,
    ):
        """

        Run a notebook in your databricks instance. This function assumes the notebook already exists in your DBFS.

        :param notebook_path: A URL that is accessible by databricks (either using DBFS or an accessible s3/GCS cluster)
        :param databricks_conn_id: Connection ID to a databricks instance
        :param existing_cluster_id: Cluster ID to existing databricks cluster for re-use.
        :param new_cluster_spec: Optional spec if you want to create a new cluste for running notebook
        :param kwargs:
        """
        self.existing_cluster_id = existing_cluster_id
        self.notebook_path = notebook_path
        self.source = source
        self.databricks_conn_id = databricks_conn_id
        self.new_cluster_spec = new_cluster_spec or {}
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        from astro.databricks.delta import DeltaDatabase

        db = DeltaDatabase(conn_id=self.databricks_conn_id)
        databricks_cluster_id = conf.get("astro_sdk", "databricks_cluster_id")
        api_client = db.api_client
        create_and_run_job(
            api_client=api_client,
            file_to_run=self.notebook_path,
            databricks_job_name="run file " + self.notebook_path,
            existing_cluster_id=databricks_cluster_id,
            new_cluster_specs=self.new_cluster_spec,
        )
