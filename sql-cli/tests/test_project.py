import os
from pathlib import Path

from sql_cli.project import Project
from tests.utils import list_dir

BASE_PATHS = [
    Path(".airflow"),
    Path(".airflow/dags"),
    Path(".airflow/dags/sql"),
    Path(".airflow/default"),
    Path(".airflow/default/airflow.cfg"),
    Path(".airflow/default/airflow.db"),
    Path(".airflow/dev"),
    Path("config"),
    Path("config/default"),
    Path("config/default/configuration.yml"),
    Path("config/dev"),
    Path("config/dev/configuration.yml"),
    Path("data"),
    Path("data/imdb.db"),
    Path("data/retail.db"),
    Path("workflows"),
    Path("workflows/example_basic_transform"),
    Path("workflows/example_basic_transform/top_animations.sql"),
    Path("workflows/example_templating"),
    Path("workflows/example_templating/filtered_orders.sql"),
    Path("workflows/example_templating/joint_orders_customers.sql"),
]


def test_initialise_project_with_dirname(tmp_path):
    Project(tmp_path).initialise()
    paths = list_dir(tmp_path.as_posix())
    assert paths == sorted(BASE_PATHS)


def test_initialise_project_in_previously_initialised_dir(tmp_path):
    Project(tmp_path).initialise()
    paths = list_dir(tmp_path.as_posix())
    assert paths == sorted(BASE_PATHS)
    Project(tmp_path).initialise()
    paths = list_dir(tmp_path.as_posix())
    assert paths == sorted(BASE_PATHS)
    # TODO: make sure we did not override the content of existing files!
