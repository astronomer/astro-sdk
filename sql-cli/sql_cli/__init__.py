import importlib.metadata
import os

os.environ["AIRFLOW__CORE__ENABLE_XCOM_PICKLING"] = "True"
__version__ = importlib.metadata.version("astro-sql-cli")
