from airflow.configuration import conf

DEFAULT_SCHEMA = "tmp_astro"
SCHEMA = conf.get("astro_sdk", "sql_schema", fallback=DEFAULT_SCHEMA)

SNOWFLAKE_STORAGE_INTEGRATION_AMAZON = conf.get(
    section="astro_sdk", key="snowflake_storage_integration_amazon", fallback=None
)

SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE = conf.get(
    section="astro_sdk",
    key="snowflake_storage_integration_google",
    fallback="gcs_int_python_sdk",
)
