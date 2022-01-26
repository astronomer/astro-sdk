import os

import pytest
import yaml
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@pytest.fixture(scope="session", autouse=True)
def create_database_connections():
    with open(os.path.dirname(__file__) + "/test-connections.yaml") as file:
        yaml_with_env = os.path.expandvars(file.read())
        yaml_dicts = yaml.safe_load(yaml_with_env)
        connections = []
        for i in yaml_dicts["connections"]:
            connections.append(Connection(**i))
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()
        session.query(Connection).delete()
        for conn in connections:
            session.add(conn)


@pytest.fixture
def sample_dag():
    yield DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()
