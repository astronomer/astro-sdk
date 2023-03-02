import logging
import os

import pytest
import yaml
from airflow.models import DAG, Connection, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session
from utils.test_utils import create_unique_str

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
UNIQUE_HASH_SIZE = 16


@pytest.fixture
def sample_dag():
    dag_id = create_unique_str(UNIQUE_HASH_SIZE)
    yield DAG(dag_id, start_date=DEFAULT_DATE)
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()


@pytest.fixture(scope="session", autouse=True)
def create_database_connections():
    with open(os.path.dirname(__file__) + "/../test-connections.yaml") as fp:
        yaml_with_env = os.path.expandvars(fp.read())
        yaml_dicts = yaml.safe_load(yaml_with_env)
        connections = []
        for i in yaml_dicts["connections"]:
            connections.append(Connection(**i))
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()
        session.query(Connection).delete()
        create_default_connections(session)
        for conn in connections:
            last_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
            if last_conn is not None:
                session.delete(last_conn)
                session.flush()
                logging.info(
                    "Overriding existing conn_id %s with connection specified in test_connections.yaml",
                    conn.conn_id,
                )
            session.add(conn)
