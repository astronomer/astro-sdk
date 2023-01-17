import importlib.metadata
import os

import typer.rich_utils

from sql_cli.utils.rich import rich_format_error

# We monkey-patch rich_format_error to make it environment aware
typer.rich_utils.rich_format_error = rich_format_error

# TODO: Remove AIRFLOW__CORE__ENABLE_XCOM_PICKLING after the `astro-sdk-python` package 1.3 is released
os.environ["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"] = "True"
os.environ["AIRFLOW__CORE__LAZY_LOAD_PLUGINS"] = "True"
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

__version__ = importlib.metadata.version("astro-sql-cli")
