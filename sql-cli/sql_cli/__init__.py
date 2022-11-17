import importlib.metadata
import os

import typer.rich_utils

from sql_cli.utils.rich import rich_format_error

# We monkey-patch rich_format_error to make it environment aware
typer.rich_utils.rich_format_error = rich_format_error

# TODO: Remove after the `astro-sdk-python` package 1.3 is released
os.environ["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"] = "True"
__version__ = importlib.metadata.version("astro-sql-cli")
