from pathlib import Path

from sql_cli.project import Project
from tests.utils import list_dir

BASE_PATHS = [
    Path(".airflow"),
    Path(".airflow/dags"),
    Path(".airflow/dags/include"),
    Path(".airflow/default"),
    Path(".airflow/default/airflow.cfg"),
    Path(".airflow/default/airflow.db"),
    Path(".airflow/dev"),
    Path(".airflow/dev/airflow.cfg"),
    Path(".airflow/dev/airflow.db"),
    Path(".env"),
    Path("config"),
    Path("config/default"),
    Path("config/default/configuration.yml"),
    Path("config/default/configuration.yml.example"),
    Path("config/dev"),
    Path("config/dev/configuration.yml"),
    Path("config/global.yml"),
    Path("data"),
    Path("data/imdb.db"),
    Path("data/retail.db"),
    Path("workflows"),
    Path("workflows/example_basic_transform"),
    Path("workflows/example_basic_transform/top_animations.sql"),
    Path("workflows/example_deploy"),
    Path("workflows/example_deploy/orders.yaml"),
    Path("workflows/example_deploy/subset.sql"),
    Path("workflows/example_deploy/workflow.yml"),
    Path("workflows/example_load_file"),
    Path("workflows/example_load_file/load_imdb_movies.yaml"),
    Path("workflows/example_load_file/transform_imdb_movies.sql"),
    Path("workflows/example_templating"),
    Path("workflows/example_templating/filtered_orders.sql"),
    Path("workflows/example_templating/joint_orders_customers.sql"),
]


def test_initialise_project_with_dirname(tmp_path):
    project = Project(tmp_path)
    project.initialise()
    assert list_dir(project.directory) == sorted(BASE_PATHS)


def test_initialise_project_in_previously_initialised_dir(initialised_project):
    project = Project(initialised_project.directory)
    project.initialise()
    assert list_dir(project.directory) == sorted(BASE_PATHS)
    # TODO: make sure we did not override the content of existing files!
