import os

DEFAULT_SCHEMA = "tmp_astro"
SCHEMA = os.getenv("AIRFLOW__ASTRO__SQL_SCHEMA") or DEFAULT_SCHEMA
