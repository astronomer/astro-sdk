import os

from astro.constants import DEFAULT_SCHEMA

SCHEMA = os.getenv("AIRFLOW__ASTRO__SQL_SCHEMA") or DEFAULT_SCHEMA
