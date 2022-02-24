import os

import pytest
from airflow.executors.debug_executor import DebugExecutor
from airflow.models.dagbag import DagBag

# from astro.sql.operators.temp_hooks import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import State
from google.api_core.exceptions import NotFound

# Import Operator

DEFAULT_SCHEMA = "ASTROFLOW_CI"
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@provide_session
def get_session(session=None):
    return session


@pytest.fixture()
def session():
    return get_session()


@pytest.mark.parametrize(
    "dag_id",
    [
        "astro_homes_etl_dag",
        "astro_test_dag",
        "find_top_rentals_with_sql_files",
        "demo_with_s3_and_csv",
        "snowflake_animal_adoption_example",
    ],
)
def test_example_dag(session, dag_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db = DagBag(dir_path + "/../example_dags")
    dag = db.get_dag(dag_id)

    if dag is None:
        raise NotFound(
            "DAG not found", detail=f"The DAG with dag_id: {dag_id} was not found"
        )

    dag.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, dag_run_state=State.NONE)

    dag.run(
        executor=DebugExecutor(),
        start_date=DEFAULT_DATE,
        end_date=DEFAULT_DATE,
        run_at_least_once=True,
    )
