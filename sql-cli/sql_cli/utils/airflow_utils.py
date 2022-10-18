import importlib
import os
from configparser import ConfigParser
from pathlib import Path

import airflow
from airflow.models.dag import DAG
from airflow.utils.cli import AirflowException, _search_for_dag_file, logger, process_subdir, settings


def retrieve_airflow_database_conn_from_config(airflow_home: Path) -> str:
    """
    Retrieve the path to the Airflow metadata database connection.

    :params airfow_home: Path to where Airflow was initialized ($AIRFLOW_HOME).
    :returns: Airflow metadata database connection URI
    """
    os.environ["AIRFLOW_HOME"] = str(airflow_home.resolve())
    filename = airflow_home / "airflow.cfg"
    parser = ConfigParser()
    parser.read(filename)
    confdict = {section: dict(parser.items(section)) for section in parser.sections()}
    return confdict["database"]["sql_alchemy_conn"]


def disable_examples(airflow_home: Path) -> None:
    """
    Disable Airflow examples in the configuration file available at the given airflow_home directory.

    :params airflow_home: Path to where Airflow was initialised ($AIRFLOW_HOME)
    """
    filename = airflow_home / "airflow.cfg"
    parser = ConfigParser()
    parser.read(filename)
    parser["core"]["load_examples"] = "False"
    parser["core"]["logging_level"] = "WARN"
    with open(filename, "w") as out_fp:
        parser.write(out_fp)


def set_airflow_database_conn(airflow_meta_conn: str) -> None:
    """
    Given a desired Airflow DB connection string, refresh Airflow settings so
    that Airflow ORM uses this database as metadata store.

    :params airflow_db_conn: Similar to `sqlite:////tmp/project/airflow.db`
    """
    # This is a hacky approcah we managed to find to make thigs work with Airflow 2.4
    os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = airflow_meta_conn
    importlib.reload(airflow)
    importlib.reload(airflow.configuration)
    importlib.reload(airflow.models.base)
    importlib.reload(airflow.models.connection)


# The following function was copied from Apache Airflow
# https://github.com/apache/airflow/commit/ce071172e22fba018889db7dcfac4a4d0fc41cda
# And we should replace by the upstream method once Airflow 2.5 is released
# We are copying it so that we do not include examples
# This helps silencing the SQL CLI output and also the speed of the run command
def get_dag(subdir: str, dag_id: str, include_examples=False) -> DAG:
    """
    Returns DAG of a given dag_id
    First it we'll try to use the given subdir.  If that doesn't work, we'll try to
    find the correct path (assuming it's a file) and failing that, use the configured
    dags folder.
    """
    from airflow.models import DagBag

    first_path = process_subdir(subdir)
    dagbag = DagBag(first_path, include_examples=include_examples)
    if dag_id not in dagbag.dags:
        fallback_path = _search_for_dag_file(subdir) or settings.DAGS_FOLDER
        logger.warning("Dag %r not found in path %s; trying path %s", dag_id, first_path, fallback_path)
        dagbag = DagBag(dag_folder=fallback_path, include_examples=include_examples)
        if dag_id not in dagbag.dags:
            raise AirflowException(
                f"Dag {dag_id!r} could not be found; either it does not exist or it failed to parse."
            )
    return dagbag.dags[dag_id]
