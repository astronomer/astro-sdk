from airflow.configuration import conf

DEFAULT_SCHEMA = "tmp_astro"
SCHEMA = conf.get("astro_python_sdk", "sql_schema") or DEFAULT_SCHEMA
