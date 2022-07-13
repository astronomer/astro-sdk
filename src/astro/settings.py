from airflow.configuration import conf

DEFAULT_SCHEMA = "tmp_astro"
SCHEMA = conf.get("astro_sdk", "sql_schema", fallback=DEFAULT_SCHEMA)

# We are not defining a fallback key on purpose. S3 Snowflake stages can also
# be created without a storage integration, by using the Airflow AWS connection
# properties.
SNOWFLAKE_STORAGE_INTEGRATION_AMAZON = conf.get(
    section="astro_sdk", key="snowflake_storage_integration_amazon", fallback=None
)

SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE = conf.get(
    section="astro_sdk", key="snowflake_storage_integration_google", fallback=None
)
