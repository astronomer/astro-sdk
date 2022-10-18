import importlib
import os
from configparser import ConfigParser
from pathlib import Path

import airflow


def retrieve_airflow_database_conn_from_config(airflow_home: Path) -> str:
    """
    Retrieve the path to the Airflow metadata database connection.

    :params airfow_home: Path to where Airflow was initialized ($AIRFLOW_HOME).
    :returns: Airflow metadata database connection URI
    """
    filename = airflow_home / "airflow.cfg"
    parser = ConfigParser()
    parser.read(filename)
    confdict = {section: dict(parser.items(section)) for section in parser.sections()}
    return confdict["database"]["sql_alchemy_conn"]


def set_airflow_database_conn(airflow_meta_conn: str) -> None:
    """
    Given a desired Airflow DB connection string, refresh Airflow settings so
    that Airflow ORM uses this database as metadata store.

    :params airflow_db_conn: Similar to `sqlite:////tmp/project/airflow.db`
    """
    # This is a hacky approcah we managed to find to make thigs work with Airflow 2.4
    # os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = airflow_meta_conn
    # importlib.reload(airflow)
    # importlib.reload(airflow.configuration)
    # importlib.reload(airflow.models.base)
    # importlib.reload(airflow.models.connection)
