from configparser import ConfigParser
from pathlib import Path


def retrieve_airflow_meta_database_conn(airflow_home: Path) -> str:
    """
    Retrieve the path to the Airflow metadata database connection.

    :params airfow_home: Path to where Airflow was initialized ($AIRFLOW_HOME).
    """
    filename = airflow_home / "airflow.cfg"
    parser = ConfigParser()
    parser.read(filename)
    confdict = {section: dict(parser.items(section)) for section in parser.sections()}
    return confdict["database"]["sql_alchemy_conn"]
