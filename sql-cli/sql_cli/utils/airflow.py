from __future__ import annotations

import importlib
import logging
import os
from configparser import ConfigParser
from pathlib import Path
from typing import TYPE_CHECKING

from packaging.version import Version

if TYPE_CHECKING:
    from airflow.models.dag import DAG  # pragma: no cover

logger = logging.getLogger(__name__)


def version() -> Version:
    """
    Return the version of Airflow installed.
    """
    import airflow  # skipcq: PYL-W0406

    return Version(airflow.__version__)


def initialise(airflow_home: Path, airflow_dags_folder: Path) -> None:
    """
    Create an Airflow database and configure airflow via environment variables.

    :params airflow_home: The airflow home to set
    :params airflow_dags_folder: The airflow dags folder to set
    """
    # Set airflow environment variables prior to database initialization
    os.environ["AIRFLOW_HOME"] = airflow_home.as_posix()
    os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = airflow_dags_folder.as_posix()
    if version() >= Version("2.3"):
        os.environ.pop("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", None)
    else:
        os.environ.pop("AIRFLOW__CORE__SQL_ALCHEMY_CONN", None)

    # Reload airflow configuration after setting environment variables
    from airflow import configuration

    importlib.reload(configuration)

    # Initialise the airflow database & hide all logs
    os.system("airflow db init > /dev/null 2>&1")  # skipcq: BAN-B605,BAN-B607


def reload(airflow_home: Path) -> None:
    """
    Given a desired Airflow home, refresh Airflow settings so
    that Airflow ORM uses the correct database as metadata store.

    :params airflow_home: The airflow home to set
    """
    # Set airflow environment variables
    os.environ["AIRFLOW_HOME"] = airflow_home.as_posix()
    # ..from config
    parser = ConfigParser()
    parser.read(airflow_home / "airflow.cfg")
    if version() >= Version("2.3"):
        os.environ["AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"] = parser.get("database", "sql_alchemy_conn")
    else:
        os.environ["AIRFLOW__CORE__SQL_ALCHEMY_CONN"] = parser.get("core", "sql_alchemy_conn")

    # Reload airflow configuration after setting environment variables
    from airflow import configuration

    importlib.reload(configuration)

    # Re-initialise airflow settings
    import airflow

    importlib.reload(airflow)


# The following function was copied from Apache Airflow
# https://github.com/apache/airflow/commit/ce071172e22fba018889db7dcfac4a4d0fc41cda
# And we should replace by the upstream method once Airflow 2.5 is released
# We are copying it so that we do not include examples
# This helps silencing the SQL CLI output and also the speed of the run command
def _search_for_dag_file(val: str) -> str:
    """
    Search for the file referenced at fileloc.
    By the time we get to this function, we've already run this `val` through `process_subdir`
    and loaded the DagBag there and came up empty.  So here, if `val` is a file path, we make
    a last ditch effort to try and find a dag file with the same name in our dags folder. (This
    avoids the unnecessary dag parsing that would occur if we just parsed the dags folder).
    If `val` is a path to a file, this likely means that the serializing process had a dags_folder
    equal to only the dag file in question. This prevents us from determining the relative location.
    And if the paths are different between worker and dag processor / scheduler, then we won't find
    the dag at the given location.
    """
    from airflow import settings

    if val and Path(val).suffix in (".zip", ".py"):
        matches = list(Path(settings.DAGS_FOLDER).rglob(Path(val).name))
        if len(matches) == 1:
            return matches[0].as_posix()
    return ""


# The following function was copied from Apache Airflow
# https://github.com/apache/airflow/commit/ce071172e22fba018889db7dcfac4a4d0fc41cda
# And we should replace by the upstream method once Airflow 2.5 is released
# We are copying it so that we do not include examples
# This helps silencing the SQL CLI output and also the speed of the run command
def get_dag(subdir: str, dag_id: str, include_examples: bool = False) -> DAG:
    """
    Returns DAG of a given dag_id
    First it we'll try to use the given subdir.  If that doesn't work, we'll try to
    find the correct path (assuming it's a file) and failing that, use the configured
    dags folder.
    """
    from airflow import settings
    from airflow.exceptions import AirflowException
    from airflow.models import DagBag
    from airflow.utils.cli import process_subdir

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


def check_for_dag_import_errors(dag_file: Path) -> dict[str, str]:
    """
    Check for import errors in DAG

    :param dag_file: The path to the dag file.

    :returns: the import errors found per DAG during processing the DagBag.
    """
    from airflow.models import DagBag
    from airflow.utils.cli import process_subdir

    dag_folder = process_subdir(str(dag_file))
    dagbag = DagBag(dag_folder, include_examples=False)
    return dagbag.import_errors
