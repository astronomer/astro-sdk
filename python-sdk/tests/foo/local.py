import argparse
import logging
import pathlib

from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.utils import timezone
from airflow.utils.cli import get_dag
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.models.dagrun import DagRunState
from sqlalchemy.orm import Session
import os

import yaml
from airflow.models import Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session
import timeit
log = logging.getLogger(__name__)

CWD = pathlib.Path(__file__).parent
DEFAULT_DATE = timezone.datetime(2022, 1, 1)


# TODO: Check webserver code to find how to pull all dependencies for a specific task
@provide_session
def local_dag_flow(
    subdir, dag_id, execution_date=timezone.utcnow(), session: Session = NEW_SESSION
):
    run_id = DagRun.generate_run_id(DagRunType.MANUAL, execution_date)
    dag = get_dag(subdir=subdir, dag_id=dag_id)
    dag.clear(dag_run_state=False)
    # TODO: ask Ash how we should handle recreating existing DAGruns
    dr = dag.create_dagrun(state=DagRunState.QUEUED, execution_date=execution_date, run_id=run_id)
    # dr = get_or_create_dagrun(dag, execution_date, run_id, session)
    tasks = dag.tasks
    # tasks.reverse()  # Reversing to test what happens when a task doesn't have dependencies met
    while tasks:
        unfinished_tasks = []
        for task in tasks:
            ti = get_or_create_taskinstance(dr, run_id, session, task)
            ti.task = task
            if ti.are_dependencies_met():
                run_task(session, ti)
            else:
                unfinished_tasks.append(task)
        tasks = unfinished_tasks


def run_task(session, ti):
    import sys
    current_task = ti.render_templates(ti.get_template_context())
    format = logging.Formatter("\t[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(format)
    if not current_task.log.handlers:  # only add log handler once
        current_task.log.addHandler(handler)
    print(f"Running task {current_task.task_id}")
    xcom_value = current_task.execute(
        context=ti.get_template_context()
    )
    ti.xcom_push(key=XCOM_RETURN_KEY, value=xcom_value, session=session)
    print(f"{current_task.task_id} ran successfully!")

    ti.set_state(State.SUCCESS)


def get_or_create_taskinstance(dr, run_id, session, task):
    ti = (
        session.query(TaskInstance)
        .filter(
            TaskInstance.task_id == task.task_id, TaskInstance.run_id == run_id
        )
        .first()
    )

# /    session.delete()
    if not ti:  # we should create the TI the first time we run it
        ti = TaskInstance(task, run_id=dr.run_id)
        ti.log.setLevel(logging.DEBUG)
        session.add(ti)
        session.flush()
    return ti


def get_or_create_dagrun(dag, execution_date, run_id, session):
    dr = (
        session.query(DagRun)
        .filter(DagRun.dag_id == dag.dag_id, DagRun.run_id == run_id)
        .first()
    )
    if not dr:
        dr = DagRun(
            dag_id=dag.dag_id,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
            run_type=DagRunType.MANUAL,
            execution_date=execution_date,
        )
        session.add(dr)
        session.flush()
    return dr


def fix_keys(conn_dict):
    new_dict = {}
    for k, v in conn_dict.items():
        if k == "conn_id" or k == "conn_type":
            new_dict[k] = v
        else:
            new_dict[k.replace("conn_", "")] = v
    return new_dict


def create_database_connections(settings_file):
    with open(settings_file) as fp:
        yaml_with_env = os.path.expandvars(fp.read())
        yaml_dicts = yaml.safe_load(yaml_with_env)
        connections = []
        for i in yaml_dicts['airflow']["connections"]:
            i = fix_keys(i)
            connections.append(Connection(**i))
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()
        session.query(Connection).delete()
        create_default_connections(session)
        for conn in connections:
            session.add(conn)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run an Airflow DAG locally")
    parser.add_argument(
        "--dag_dir", metavar="dag_dir", required=True, help="The path to the DAG file you want to parse"
    )
    parser.add_argument(
        "--dag_id", metavar="dag_id", required=True, help="The dag_id of the DAG you want to run"
    )
    parser.add_argument(
        "--settings_file", metavar="dag_id", required=False, default=None, help="The dag_id of the DAG you want to run"
    )

    parser.add_argument(
        "--execution_date",
        metavar="execution_date",
        required=False,
        default=timezone.utcnow(),
        help="The execution date of the DAG you're running",
    )
    args = parser.parse_args()

    if args.settings_file:
        create_database_connections(args.settings_file)

    start = timeit.default_timer()
    local_dag_flow(args.dag_dir, args.dag_id, args.execution_date)
    stop = timeit.default_timer()
    print('Time: ', stop - start)