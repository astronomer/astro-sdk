from airflow.configuration import conf

DEFAULT_SCHEMA = "tmp_astro"
SCHEMA = conf.get("astro_sdk", "sql_schema", fallback=None) or DEFAULT_SCHEMA
