from pathlib import Path

from airflow.configuration import conf

PROJECT_BASE_PATH = Path(__file__).parent.parent.resolve()

# This needs to be updated with the `astro dev init` command to set the appropriate project directory.
SQL_CLI_PROJECT_DIRECTORY = Path(
    conf.get("astro_sdk", "sql_cli_project_directory", fallback=PROJECT_BASE_PATH / "tests")
)
