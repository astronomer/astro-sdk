from airflow.configuration import conf

DEFAULT_SCHEMA = "tmp_astro"
SCHEMA = conf.get("astro_sdk", "sql_schema", fallback=DEFAULT_SCHEMA)

ALLOW_UNSAFE_DF_STORAGE = conf.getboolean(
    "astro_sdk", "dataframe_allow_unsafe_storage", fallback=False
)

XCOM_BACKEND = conf.get("core", "xcom_backend")
IS_CUSTOM_XCOM_BACKEND = XCOM_BACKEND != "airflow.models.xcom.BaseXCom"

# We are not defining a fallback key on purpose. S3 Snowflake stages can also
# be created without a storage integration, by using the Airflow AWS connection
# properties.
SNOWFLAKE_STORAGE_INTEGRATION_AMAZON = conf.get(
    section="astro_sdk", key="snowflake_storage_integration_amazon", fallback=None
)

SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE = conf.get(
    section="astro_sdk", key="snowflake_storage_integration_google", fallback=None
)

#: How many file rows should be loaded to infer the table columns types
LOAD_TABLE_AUTODETECT_ROWS_COUNT = conf.getint(
    section="astro_sdk", key="load_table_autodetect_rows_count", fallback=1000
)
