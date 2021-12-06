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

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.sql.table import Table
from astro.utils.snowflake_merge_func import (
    is_valid_snow_identifier,
    snowflake_merge_func,
)

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
        cwd = pathlib.Path(__file__).parent
        aql.load_file(
            path=str(cwd) + "/../data/homes_merge_1.csv",
            output_table=Table(
                table_name="merge_test_1",
                conn_id="snowflake_conn",
            ),
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cwd) + "/../data/homes_merge_2.csv",
            output_table=Table(
                table_name="merge_test_2",
                conn_id="snowflake_conn",
            ),
        ).operator.execute({"run_id": "foo"})
        super().setUp()

    def test_merge_func(self):
        sql, parameters = snowflake_merge_func(
            target_table="test_merge_1",
            merge_table="test_merge_2",
            merge_keys={"sell": "sell"},
            target_columns=["sell"],
            merge_columns=["sell"],
            conflict_strategy="update",
        )

        assert (
            sql
            == "merge into Identifier(%(main_table)s) using Identifier(%(merge_table)s) "
            "on Identifier(%(merge_clause_target_0)s)=Identifier(%(merge_clause_append_0)s) "
            "when matched then UPDATE SET test_merge_1.sell=test_merge_2.sell "
            "when not matched then insert(test_merge_1.sell) values (test_merge_2.sell)"
        )
        assert parameters == {
            "merge_clause_target_0": "test_merge_1.sell",
            "merge_clause_append_0": "test_merge_2.sell",
            "main_table": "test_merge_1",
            "merge_table": "test_merge_2",
        }

    def test_merge_func_multiple_clause(self):
        sql, parameters = snowflake_merge_func(
            target_table="test_merge_1",
            merge_table="test_merge_2",
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
            "when matched then UPDATE SET test_merge_1.sell=test_merge_2.sell "
            "when not matched then insert(test_merge_1.sell) values (test_merge_2.sell)"
        )
        assert parameters == {
            "main_table": "test_merge_1",
            "merge_clause_append_0": "test_merge_2.sell",
            "merge_clause_append_1": "test_merge_2.bar",
            "merge_clause_target_0": "test_merge_1.sell",
            "merge_clause_target_1": "test_merge_1.foo",
            "merge_table": "test_merge_2",
        }

    def test_merge_fun_ignore(self):
        sql, parameters = snowflake_merge_func(
            target_table="test_merge_1",
            merge_table="test_merge_2",
            merge_keys={"sell": "sell"},
            target_columns=["sell"],
            merge_columns=["sell"],
            conflict_strategy="ignore",
        )

        assert (
            sql
            == "merge into Identifier(%(main_table)s) using Identifier(%(merge_table)s) "
            "on Identifier(%(merge_clause_target_0)s)=Identifier(%(merge_clause_append_0)s) "
            "when not matched then insert(test_merge_1.sell) values (test_merge_2.sell)"
        )
        assert parameters == {
            "merge_clause_target_0": "test_merge_1.sell",
            "merge_clause_append_0": "test_merge_2.sell",
            "main_table": "test_merge_1",
            "merge_table": "test_merge_2",
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

    def test_merge_basic_single_key(self):
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
        )
        a = aql.merge(
            target_table="merge_test_1",
            merge_table="merge_test_2",
            merge_keys={"list": "list"},
            target_columns=["list"],
            merge_columns=["list"],
            conn_id="snowflake_conn",
            conflict_strategy="ignore",
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql="SELECT * FROM merge_test_1")
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
        )
        a = aql.merge(
            target_table="merge_test_1",
            merge_table="merge_test_2",
            merge_keys={"list": "list", "sell": "sell"},
            target_columns=["list", "sell"],
            merge_columns=["list", "sell"],
            conn_id="snowflake_conn",
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            conflict_strategy="ignore",
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql="SELECT * FROM merge_test_1")
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
            warehouse="TRANSFORMING_DEV",
        )
        a = aql.merge(
            target_table="merge_test_1",
            merge_table="merge_test_2",
            merge_keys={"list": "list", "sell": "sell"},
            target_columns=["list", "sell", "taxes"],
            merge_columns=["list", "sell", "age"],
            conn_id="snowflake_conn",
            conflict_strategy="update",
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse="TRANSFORMING_DEV",
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql="SELECT * FROM merge_test_1")
        assert df.TAXES.to_list() == [1, 1, 1, 1, 1]
        assert df.AGE.to_list()[1:] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.AGE.to_list()[0])
