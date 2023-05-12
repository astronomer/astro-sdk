import tempfile

from airflow.configuration import conf
from airflow.version import version as airflow_version
from packaging.version import Version

from astro.constants import DEFAULT_SCHEMA

# Section name for astro SDK configs in airflow.cfg
SECTION_KEY = "astro_sdk"
# Bigquery list of all the valid locations: https://cloud.google.com/bigquery/docs/locations
DEFAULT_BIGQUERY_SCHEMA_LOCATION = "us"

SCHEMA = conf.get(SECTION_KEY, "sql_schema", fallback=DEFAULT_SCHEMA)
POSTGRES_SCHEMA = conf.get(SECTION_KEY, "postgres_default_schema", fallback=SCHEMA)
BIGQUERY_SCHEMA = conf.get(SECTION_KEY, "bigquery_default_schema", fallback=SCHEMA)
SNOWFLAKE_SCHEMA = conf.get(SECTION_KEY, "snowflake_default_schema", fallback=SCHEMA)
REDSHIFT_SCHEMA = conf.get(SECTION_KEY, "redshift_default_schema", fallback=SCHEMA)
MSSQL_SCHEMA = conf.get(SECTION_KEY, "mssql_default_schema", fallback=SCHEMA)
MYSQL_SCHEMA = conf.get(SECTION_KEY, "mysql_default_schema", fallback=SCHEMA)

BIGQUERY_SCHEMA_LOCATION = conf.get(
    SECTION_KEY, "bigquery_dataset_location", fallback=DEFAULT_BIGQUERY_SCHEMA_LOCATION
)

LOAD_FILE_ENABLE_NATIVE_FALLBACK = conf.get(SECTION_KEY, "load_file_enable_native_fallback", fallback=False)

DATAFRAME_STORAGE_CONN_ID = conf.get(SECTION_KEY, "xcom_storage_conn_id", fallback=None)
DATAFRAME_STORAGE_URL = conf.get(SECTION_KEY, "xcom_storage_url", fallback=tempfile.gettempdir())
STORE_DATA_LOCAL_DEV = conf.get(SECTION_KEY, "store_data_local_dev", fallback=False)

# Size is in KB - Max memory usage for Dataframe to be stored in the Airflow metadata DB
# If dataframe size is more than that, it will be stored in an Object Store defined by
# DATAFRAME_STORAGE_CONN_ID & DATAFRAME_STORAGE_URL above
MAX_DATAFRAME_MEMORY_FOR_XCOM_DB = conf.getint(SECTION_KEY, "max_dataframe_mem_for_xcom_db", fallback=100)

OPENLINEAGE_EMIT_TEMP_TABLE_EVENT = conf.getboolean(
    SECTION_KEY, "openlineage_emit_temp_table_event", fallback=True
)
OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE = conf.getboolean(
    SECTION_KEY, "openlineage_airflow_disable_source_code", fallback=True
)
XCOM_BACKEND = conf.get("core", "xcom_backend")
IS_BASE_XCOM_BACKEND = conf.get("core", "xcom_backend") == "airflow.models.xcom.BaseXCom"
ENABLE_XCOM_PICKLING = conf.getboolean("core", "enable_xcom_pickling")
AIRFLOW_25_PLUS = Version(airflow_version) >= Version("2.5.0")
# We only need AstroPandasDataframe and other custom serialization and deserialization
# if Airflow >= 2.5 and Pickling is not enabled and neither Custom XCom backend is used
NEED_CUSTOM_SERIALIZATION = AIRFLOW_25_PLUS and IS_BASE_XCOM_BACKEND and not ENABLE_XCOM_PICKLING

IS_CUSTOM_XCOM_BACKEND = XCOM_BACKEND not in [
    "airflow.models.xcom.BaseXCom",
    "astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend",
]
# We are not defining a fallback key on purpose. S3 Snowflake stages can also
# be created without a storage integration, by using the Airflow AWS connection
# properties.
SNOWFLAKE_STORAGE_INTEGRATION_AMAZON = conf.get(
    section=SECTION_KEY, key="snowflake_storage_integration_amazon", fallback=None
)

SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE = conf.get(
    section=SECTION_KEY, key="snowflake_storage_integration_google", fallback=None
)

#: How many file rows should be loaded to infer the table columns types
LOAD_TABLE_AUTODETECT_ROWS_COUNT = conf.getint(
    section=SECTION_KEY, key="load_table_autodetect_rows_count", fallback=1000
)

#: Reduce responses sizes returned by aql.run_raw_sql to avoid trashing the Airflow DB if the BaseXCom is used.
RAW_SQL_MAX_RESPONSE_SIZE = conf.getint(section=SECTION_KEY, key="run_raw_sql_response_size", fallback=-1)

# Temp changes
# Should Astro SDK automatically add inlets/outlets to take advantage of Airflow 2.4 Data-aware scheduling
AUTO_ADD_INLETS_OUTLETS = conf.getboolean(SECTION_KEY, "auto_add_inlets_outlets", fallback=True)

ASSUME_SCHEMA_EXISTS = False


def reload():
    """
    Reload settings from environment variable during runtime.
    """
    global ASSUME_SCHEMA_EXISTS  # skipcq: PYL-W0603
    ASSUME_SCHEMA_EXISTS = conf.getboolean(SECTION_KEY, "assume_schema_exists", fallback=False)


reload()
