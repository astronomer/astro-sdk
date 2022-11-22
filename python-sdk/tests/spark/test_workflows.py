from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from astro.spark.workflows.workflow_taskgroup import DatabricksNotebookOperator, DatabricksWorkflowTaskGroup


def test_basic_databricks_workflow(sample_dag):
    job_cluster = {
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

    with sample_dag:
        first_task = EmptyOperator(task_id="first_task")
        db = DatabricksWorkflowTaskGroup(
            group_id="my-workflow", job_cluster_json=job_cluster, databricks_conn_id="my_databricks_conn"
        )
        with db:
            notebook = DatabricksNotebookOperator(task_id="bar", notebook_path="/foo/bar", source="WORKSPACE")
            first_task >> notebook
    print("\n\n\n")
    assert first_task.upstream_task_ids == set()
    assert sample_dag.get_task(task_id="my-workflow.launch").upstream_task_ids == {"first_task"}
    assert sample_dag.get_task(task_id="my-workflow.bar").upstream_task_ids == {
        "my-workflow.launch",
        "first_task",
    }
    from tests.sql.operators.utils import run_dag

    run_dag(sample_dag)
