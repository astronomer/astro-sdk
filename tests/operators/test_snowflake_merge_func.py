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
import os
import pathlib
import unittest.mock

from airflow.utils import timezone

# Import Operator
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
        super().setUpClass()

    def setUp(self):
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
        super().setUp()

    def tearDown(self):
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

    def test_merge_func(self):
        sql, parameters = snowflake_merge_func(
            target_table=self.main_table,
            merge_table=self.merge_table,
            merge_keys={"sell": "sell"},
            target_columns=["sell"],
            merge_columns=["sell"],
            conflict_strategy="update",
        )

        assert (
            sql
            == "merge into Identifier(%(main_table)s) using Identifier(%(merge_table)s) "
            "on Identifier(%(merge_clause_target_0)s)=Identifier(%(merge_clause_append_0)s) "
            f"when matched then UPDATE SET {self.main_table_name}.sell={self.merge_table_name}.sell "
            f"when not matched then insert({self.main_table_name}.sell) values ({self.merge_table_name}.sell)"
        )
        assert parameters == {
            "merge_clause_target_0": f"{self.main_table_name}.sell",
            "merge_clause_append_0": f"{self.merge_table_name}.sell",
            "main_table": self.main_table,
            "merge_table": self.merge_table,
        }

    def test_merge_func_multiple_clause(self):
        sql, parameters = snowflake_merge_func(
            target_table=self.main_table,
            merge_table=self.merge_table,
            merge_keys={"sell": "sell", "foo": "bar"},
            target_columns=["sell"],
            merge_columns=["sell"],
            conflict_strategy="update",
        )

        assert (
            sql
            == "merge into Identifier(%(main_table)s) using Identifier(%(merge_table)s) "
            "on Identifier(%(merge_clause_target_0)s)=Identifier(%(merge_clause_append_0)s) AND "
            "Identifier(%(merge_clause_target_1)s)=Identifier(%(merge_clause_append_1)s) "
            f"when matched then UPDATE SET {self.main_table_name}.sell={self.merge_table_name}.sell "
            f"when not matched then insert({self.main_table_name}.sell) values ({self.merge_table_name}.sell)"
        )
        assert parameters == {
            "merge_clause_append_0": f"{self.merge_table_name}.sell",
            "merge_clause_append_1": f"{self.merge_table_name}.bar",
            "merge_clause_target_0": f"{self.main_table_name}.sell",
            "merge_clause_target_1": f"{self.main_table_name}.foo",
            "main_table": self.main_table,
            "merge_table": self.merge_table,
        }

    def test_merge_fun_ignore(self):
        sql, parameters = snowflake_merge_func(
            target_table=self.main_table,
            merge_table=self.merge_table,
            merge_keys={"sell": "sell"},
            target_columns=["sell"],
            merge_columns=["sell"],
            conflict_strategy="ignore",
        )

        assert (
            sql
            == "merge into Identifier(%(main_table)s) using Identifier(%(merge_table)s) "
            "on Identifier(%(merge_clause_target_0)s)=Identifier(%(merge_clause_append_0)s) "
            f"when not matched then insert({self.main_table_name}.sell) values ({self.merge_table_name}.sell)"
        )
        assert parameters == {
            "merge_clause_target_0": f"{self.main_table_name}.sell",
            "merge_clause_append_0": f"{self.merge_table_name}.sell",
            "main_table": self.main_table,
            "merge_table": self.merge_table,
        }

    def test_is_valid_snow_identifier(self):
        valid_strings = [
            "ValidName",
            "Valid_Name",
            '"$valid"',
            '"Valid Name"',
            '"Valid With ""Quotes"""',
            '"\x00 Valid with ""Quotes"" #$@(TSDGfsd"',
        ]
        invalid_strings = [
            "$invalid",
            "Infvalid\x00" "Invalid Name",
            '"Invalid " Name"',
            '"Also Invalid Name""',
        ]

        for v in valid_strings:
            assert is_valid_snow_identifier(v)

        for i in invalid_strings:
            assert not is_valid_snow_identifier(i)
