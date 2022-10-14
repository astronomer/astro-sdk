from unittest import mock

import pendulum
from airflow.models import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone


def create_context(task):
    dag = DAG(dag_id="dag")
    tzinfo = pendulum.timezone("UTC")
    execution_date = timezone.datetime(2022, 1, 1, 1, 0, 0, tzinfo=tzinfo)
    dag_run = DagRun(dag_id=dag.dag_id, execution_date=execution_date)
    task_instance = TaskInstance(task=task)
    task_instance.dag_run = dag_run
    task_instance.xcom_push = mock.Mock()
    return {
        "dag": dag,
        "ts": execution_date.isoformat(),
        "task": task,
        "ti": task_instance,
        "task_instance": task_instance,
        "run_id": dag_run.run_id,
        "execution_date": execution_date,
    }
