import os

DEFAULT_CHUNK_SIZE = 1000000
PYPI_PROJECT_NAME = "astro-projects"
DEFAULT_SCHEMA = os.getenv("AIRFLOW__ASTRO__SQL_SCHEMA") or "astroflow_ci"
