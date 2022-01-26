"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import math
import os
import pathlib
import unittest.mock

from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
import astro.sql as aql

# Import Operator
from astro import sql as aql
from astro.sql.table import Table
from astro.utils.snowflake_merge_func import (
    is_valid_snow_identifier,
    snowflake_merge_func,
)
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


# Mock the `conn_sample` Airflow connection
def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


class TestSnowflakeMerge(unittest.TestCase):
    """
    Test Sample Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        cwd = pathlib.Path(__file__).parent
        cls.merge_test_raw_1 = test_utils.get_table_name("merge_test_raw_1")
        main_table = Table(
            table_name=cls.merge_test_raw_1,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        cls.merge_test_raw_2 = test_utils.get_table_name("merge_test_raw_2")
        merge_table = Table(
            table_name=cls.merge_test_raw_2,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        aql.load_file(
            path=str(cwd) + "/../data/homes_merge_1.csv",
            output_table=main_table,
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cwd) + "/../data/homes_merge_2.csv", output_table=merge_table
        ).operator.execute({"run_id": "foo"})
        super().setUpClass()

    @classmethod
    def tearDownClass(cls):
        test_utils.drop_table_snowflake(
            table_name=cls.merge_test_raw_1,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        test_utils.drop_table_snowflake(
            table_name=cls.merge_test_raw_2,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

    def setUp(self):
        cwd = pathlib.Path(__file__).parent
        main_raw_table = Table(
            table_name=self.merge_test_raw_1,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        merge_raw_table = Table(
            table_name=self.merge_test_raw_2,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        self.main_table_name = test_utils.get_table_name("merge_test_1")
        self.main_table = Table(
            table_name=self.main_table_name,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        self.merge_table_name = test_utils.get_table_name("merge_test_2")
        self.merge_table = Table(
            table_name=self.merge_table_name,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )

        @aql.transform
        def fill_table(input_table: Table):
            return "SELECT * FROM {input_table}"

        with dag:
            main = fill_table(input_table=main_raw_table, output_table=self.main_table)
            merge = fill_table(
                input_table=merge_raw_table, output_table=self.merge_table
            )

        test_utils.run_dag(dag)
        super().setUp()

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

        test_utils.drop_table_snowflake(
            table_name=self.main_table_name,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        test_utils.drop_table_snowflake(
            table_name=self.merge_table_name,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

    def test_merge_basic_single_key(self):
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        )
        a = aql.merge(
            target_table=self.main_table,
            merge_table=self.merge_table,
            merge_keys={"list": "list"},
            target_columns=["list"],
            merge_columns=["list"],
            conflict_strategy="ignore",
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql=f"SELECT * FROM {self.main_table_name}")
        assert df.AGE.to_list()[1:] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.AGE.to_list()[0])
        assert df.TAXES.to_list()[1:] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert math.isnan(df.TAXES.to_list()[0])
        assert df.TAXES.to_list()[1:] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert df.LIST.to_list() == [240, 160, 180, 132, 140]
        assert df.SELL.to_list()[1:] == [142, 175, 129, 138]
        assert math.isnan(df.SELL.to_list()[0])

    def test_merge_basic_ignore(self):
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        )
        a = aql.merge(
            target_table=self.main_table,
            merge_table=self.merge_table,
            merge_keys={"list": "list", "sell": "sell"},
            target_columns=["list", "sell"],
            merge_columns=["list", "sell"],
            conn_id="snowflake_conn",
            conflict_strategy="ignore",
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql=f"SELECT * FROM {self.main_table_name}")
        assert df.AGE.to_list()[1:] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.AGE.to_list()[0])
        assert df.TAXES.to_list()[1:] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert math.isnan(df.TAXES.to_list()[0])
        assert df.TAXES.to_list()[1:] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert df.LIST.to_list() == [240, 160, 180, 132, 140]
        assert df.SELL.to_list() == [232, 142, 175, 129, 138]

    def test_merge_basic_update(self):
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        )
        a = aql.merge(
            target_table=self.main_table,
            merge_table=self.merge_table,
            merge_keys={"list": "list", "sell": "sell"},
            target_columns=["list", "sell", "taxes"],
            merge_columns=["list", "sell", "age"],
            conn_id="snowflake_conn",
            conflict_strategy="update",
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql=f"SELECT * FROM {self.main_table_name}")
        assert df.TAXES.to_list() == [1, 1, 1, 1, 1]
        assert df.AGE.to_list()[1:] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.AGE.to_list()[0])
