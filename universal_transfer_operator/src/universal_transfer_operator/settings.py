import tempfile

from airflow.configuration import conf
from airflow.version import version as airflow_version
from packaging.version import Version

# Section name for universal transfer operator configs in airflow.cfg
SECTION_KEY = "universal_transfer_operator"

DATAFRAME_STORAGE_CONN_ID = conf.get(SECTION_KEY, "xcom_storage_conn_id", fallback=None)
DATAFRAME_STORAGE_URL = conf.get(SECTION_KEY, "xcom_storage_url", fallback=tempfile.gettempdir())


# Size is in KB - Max memory usage for Dataframe to be stored in the Airflow metadata DB
# If dataframe size is more than that, it will be stored in an Object Store defined by
# DATAFRAME_STORAGE_CONN_ID & DATAFRAME_STORAGE_URL above
MAX_DATAFRAME_MEMORY_FOR_XCOM_DB = conf.getint(SECTION_KEY, "max_dataframe_mem_for_xcom_db", fallback=100)

ENABLE_XCOM_PICKLING = conf.getboolean("core", "enable_xcom_pickling")
IS_BASE_XCOM_BACKEND = conf.get("core", "xcom_backend") == "airflow.models.xcom.BaseXCom"

AIRFLOW_25_PLUS = Version(airflow_version) >= Version("2.5.0")
# We only need PandasDataframe and other custom serialization and deserialization
# if Airflow >= 2.5 and Pickling is not enabled and neither Custom XCom backend is used
NEED_CUSTOM_SERIALIZATION = AIRFLOW_25_PLUS and IS_BASE_XCOM_BACKEND and not ENABLE_XCOM_PICKLING
