import pytest
from airflow.models import Connection, DagRun, TaskInstance as TI
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session


@pytest.fixture(scope="session", autouse=True)
def create_database_connections():
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()
        session.query(Connection).delete()
        create_default_connections(session)
