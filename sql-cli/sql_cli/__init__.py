import importlib.metadata
import os

# TODO: Remove after the `astro-sdk-python` package 1.3 is released
os.environ["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"] = "True"
__version__ = importlib.metadata.version("astro-sql-cli")
