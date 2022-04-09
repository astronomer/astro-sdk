import pytest

"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import pathlib
import sys
import unittest.mock

from airflow.utils import timezone

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)

original_import = __import__


def import_mock(name, *args):
    if name in [
        "airflow.providers.google.cloud.hooks.bigquery",
        "airflow.providers.postgres.hooks.postgres",
        "airflow.providers.snowflake.hooks.snowflake",
    ]:
        raise ModuleNotFoundError
    return original_import(name, *args)


class TestMissingPackages(unittest.TestCase):
    """
    Test Missing packages.

    NOTE - These testcases will fail in case if we import any dependencies in 'astro/__init__.py'
            which directly import or import a package which import below-mentioned packages.

            airflow.providers.google.cloud.hooks.bigquery
            airflow.providers.postgres.hooks.postgres
            airflow.providers.snowflake.hooks.snowflake
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        # Removed cached module from sys.module to make any
        # code post mocking 'astro.utils.dependencies' work.
        sys.modules.pop("astro.utils.dependencies", None)
        super().setUpClass()

    def setUp(self):
        super().setUp()

    def test_missing_bigquery_package(self):
        with unittest.mock.patch("builtins.__import__", import_mock):
            from astro.utils.dependencies import BigQueryHook

            with pytest.raises(RuntimeError) as error:
                BigQueryHook.conn_type

            assert (
                str(error.value)
                == "Error loading the package apache-airflow-providers-google,"
                " please make sure all the dependencies are installed. try - pip install"
                " astro-projects[google]"
            )

    def test_missing_postgres_package(self):
        with unittest.mock.patch("builtins.__import__", import_mock):
            from astro.utils.dependencies import PostgresHook

            with pytest.raises(RuntimeError) as error:
                PostgresHook.conn_type

            assert (
                str(error.value)
                == "Error loading the package apache-airflow-providers-postgres,"
                " please make sure all the dependencies are installed. try - pip install"
                " astro-projects[postgres]"
            )

    def test_missing_snowflake_package(self):
        with unittest.mock.patch("builtins.__import__", import_mock):
            from astro.utils.dependencies import SnowflakeHook

            with pytest.raises(RuntimeError) as error:
                SnowflakeHook.conn_type

            assert (
                str(error.value)
                == "Error loading the package apache-airflow-providers-snowflake,"
                " please make sure all the dependencies are installed. try - pip install"
                " astro-projects[snowflake]"
            )
