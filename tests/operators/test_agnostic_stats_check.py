"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import os
import pathlib
import unittest.mock

from airflow.models import DAG
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.sql.table import Table
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestStatsCheckOperator(unittest.TestCase):
    """
    Test Stats Check Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes.csv",
            output_table=Table(
                "stats_check_test_1",
                conn_id="postgres_conn",
                database="pagila",
                schema="public",
            ),
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes2.csv",
            output_table=Table(
                "stats_check_test_2",
                conn_id="postgres_conn",
                database="pagila",
                schema="public",
            ),
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes3.csv",
            output_table=Table(
                "stats_check_test_3",
                conn_id="postgres_conn",
                database="pagila",
                schema="public",
            ),
        ).operator.execute({"run_id": "foo"})

        cls.Stats_check_table_4 = test_utils.get_table_name("stats_check_test_4")
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes.csv",
            output_table=Table(
                table_name=cls.Stats_check_table_4,
                conn_id="snowflake_conn",
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            ),
        ).operator.execute({"run_id": "foo"})
        cls.Stats_check_table_5 = test_utils.get_table_name("stats_check_test_5")
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes2.csv",
            output_table=Table(
                table_name=cls.Stats_check_table_5,
                conn_id="snowflake_conn",
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            ),
        ).operator.execute({"run_id": "foo"})

        cls.Stats_check_table_6 = test_utils.get_table_name("stats_check_test_6")
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes3.csv",
            output_table=Table(
                table_name=cls.Stats_check_table_6,
                conn_id="snowflake_conn",
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            ),
        ).operator.execute({"run_id": "foo"})

    @classmethod
    def tearDownClass(cls) -> None:
        test_utils.drop_table_snowflake(
            table_name=cls.Stats_check_table_4,  # type: ignore
            database=os.getenv("SNOWFLAKE_DATABASE"),  # type: ignore
            schema=os.getenv("SNOWFLAKE_SCHEMA"),  # type: ignore
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # type: ignore
            conn_id="snowflake_conn",
        )
        test_utils.drop_table_snowflake(
            table_name=cls.Stats_check_table_5,  # type: ignore
            database=os.getenv("SNOWFLAKE_DATABASE"),  # type: ignore
            schema=os.getenv("SNOWFLAKE_SCHEMA"),  # type: ignore
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # type: ignore
            conn_id="snowflake_conn",
        )
        test_utils.drop_table_snowflake(
            table_name=cls.Stats_check_table_6,  # type: ignore
            database=os.getenv("SNOWFLAKE_DATABASE"),  # type: ignore
            schema=os.getenv("SNOWFLAKE_SCHEMA"),  # type: ignore
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # type: ignore
            conn_id="snowflake_conn",
        )

    def clear_run(self):
        self.run = False

    def setUp(self):
        super().setUp()
        self.clear_run()
        self.addCleanup(self.clear_run)
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )

    def test_stats_check_postgres_outlier_exists(self):
        try:
            a = aql.stats_check(
                main_table=Table(
                    "stats_check_test_1",
                    database="pagila",
                    conn_id="postgres_conn",
                    schema="public",
                ),
                compare_table=Table(
                    "stats_check_test_2",
                    database="pagila",
                    conn_id="postgres_conn",
                    schema="public",
                ),
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError as e:
            assert True

    def test_stats_check_postgres_outlier_not_exists(self):
        try:
            a = aql.stats_check(
                main_table=Table(
                    "stats_check_test_1",
                    database="pagila",
                    conn_id="postgres_conn",
                    schema="public",
                ),
                compare_table=Table(
                    "stats_check_test_3",
                    database="pagila",
                    conn_id="postgres_conn",
                    schema="public",
                ),
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError as e:
            assert False

    def test_stats_check_snowflake_outlier_exists(self):
        try:
            a = aql.stats_check(
                main_table=Table(
                    table_name=self.Stats_check_table_4,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                compare_table=Table(
                    table_name=self.Stats_check_table_5,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError as e:
            assert True

    def test_stats_check_snowflake_outlier_not_exists(self):
        try:
            a = aql.stats_check(
                main_table=Table(
                    table_name=self.Stats_check_table_4,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                compare_table=Table(
                    table_name=self.Stats_check_table_6,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                checks=[
                    aql.OutlierCheck(
                        "room_check", {"rooms": "rooms", "taxes": "taxes"}, 2, 0.0
                    )
                ],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError as e:
            assert False


class TestBIGQueryIntegrationWithStatsCheckOperator(unittest.TestCase):
    """
    Test Stats Check Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes.csv",
            output_table=Table(
                "stats_check_test_1", conn_id="bigquery", schema="tmp_astro"
            ),
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes2.csv",
            output_table=Table(
                "stats_check_test_2", conn_id="bigquery", schema="tmp_astro"
            ),
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes3.csv",
            output_table=Table(
                "stats_check_test_3", conn_id="bigquery", schema="tmp_astro"
            ),
        ).operator.execute({"run_id": "foo"})

    @classmethod
    def tearDownClass(cls) -> None:
        pass

    def clear_run(self):
        self.run = False

    def setUp(self):
        super().setUp()
        self.clear_run()
        self.addCleanup(self.clear_run)
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )

    def test_stats_check_bigQuery_outlier_exists(self):
        try:
            a = aql.stats_check(
                main_table=Table(
                    "stats_check_test_1", conn_id="bigquery", schema="tmp_astro"
                ),
                compare_table=Table(
                    "stats_check_test_2", conn_id="bigquery", schema="tmp_astro"
                ),
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError as e:
            assert True

    def test_stats_check_postgres_bigQuery_not_exists(self):
        try:
            a = aql.stats_check(
                main_table=Table(
                    "stats_check_test_1", conn_id="bigquery", schema="tmp_astro"
                ),
                compare_table=Table(
                    "stats_check_test_3", conn_id="bigquery", schema="tmp_astro"
                ),
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError as e:
            assert False
