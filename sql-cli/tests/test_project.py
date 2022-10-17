from pathlib import Path
from tempfile import TemporaryDirectory

from sql_cli.project import Project
from tests.utils import list_dir

BASE_PATHS = [
    Path(".airflow"),
    Path(".airflow/dags"),
    Path(".airflow/dags/sql"),
    Path(".airflow/default"),
    Path(".airflow/dev"),
    Path("config"),
    Path("config/default"),
    Path("config/default/configuration.yml"),
    Path("config/dev"),
    Path("config/dev/configuration.yml"),
    Path("data"),
    Path("data/movies.db"),
    Path("data/retail.db"),
    Path("workflows"),
    Path("workflows/example_basic_transform"),
    Path("workflows/example_basic_transform/top_animations.sql"),
    Path("workflows/example_templating"),
    Path("workflows/example_templating/filtered_orders.sql"),
    Path("workflows/example_templating/joint_orders_customers.sql"),
]


def test_initialise_project_with_dirname():
    with TemporaryDirectory() as dir_name:
        Project(dir_name).initialise()
        paths = list_dir(dir_name)
        assert all(base_path in paths for base_path in BASE_PATHS)


def test_initialise_project_in_previously_initialised_dir():
    with TemporaryDirectory() as dir_name:
        Project(dir_name).initialise()
        paths = list_dir(dir_name)
        assert all(base_path in paths for base_path in BASE_PATHS)
        Project(dir_name).initialise()
        paths = list_dir(dir_name)
        assert all(base_path in paths for base_path in BASE_PATHS)
        # TODO: make sure we did not override the content of existing files!
