import tempfile

from airflow.configuration import conf

from astro.constants import DEFAULT_SCHEMA

SCHEMA = conf.get("astro_sdk", "sql_schema", fallback=DEFAULT_SCHEMA)
POSTGRES_SCHEMA = conf.get("astro_sdk", "postgres_default_schema", fallback=SCHEMA)
BIGQUERY_SCHEMA = conf.get("astro_sdk", "bigquery_default_schema", fallback=SCHEMA)
SNOWFLAKE_SCHEMA = conf.get("astro_sdk", "snowflake_default_schema", fallback=SCHEMA)
REDSHIFT_SCHEMA = conf.get("astro_sdk", "redshift_default_schema", fallback=SCHEMA)

LOAD_FILE_ENABLE_NATIVE_FALLBACK = conf.get("astro_sdk", "load_file_enable_native_fallback", fallback=False)

DATAFRAME_STORAGE_CONN_ID = conf.get("astro_sdk", "xcom_storage_conn_id", fallback=None)
DATAFRAME_STORAGE_URL = conf.get("astro_sdk", "xcom_storage_url", fallback=tempfile.gettempdir())
STORE_DATA_LOCAL_DEV = conf.get("astro_sdk", "store_data_local_dev", fallback=False)
OPENLINEAGE_EMIT_TEMP_TABLE_EVENT = conf.getboolean(
    "astro_sdk", "openlineage_emit_temp_table_event", fallback=True
)
XCOM_BACKEND = conf.get("core", "xcom_backend")
IS_CUSTOM_XCOM_BACKEND = XCOM_BACKEND not in [
    "airflow.models.xcom.BaseXCom",
    "astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend",
]
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


#: Reduce responses sizes returned by aql.run_raw_sql to avoid trashing the Airflow DB if the BaseXCom is used.
RAW_SQL_MAX_RESPONSE_SIZE = conf.getint(section="astro_sdk", key="run_raw_sql_response_size", fallback=-1)

# Temp changes
# Should Astro SDK automatically add inlets/outlets to take advantage of Airflow 2.4 Data-aware scheduling
AUTO_ADD_INLETS_OUTLETS = conf.getboolean("astro_sdk", "auto_add_inlets_outlets", fallback=True)
