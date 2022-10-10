from pathlib import Path

from airflow.configuration import conf

# This needs to be updated with the `astro dev init` command to set the appropriate project directory.
SQL_CLI_PROJECT_DIRECTORY = Path(
    conf.get("astro_sdk", "sql_cli_project_directory", fallback="../tests/")
)
