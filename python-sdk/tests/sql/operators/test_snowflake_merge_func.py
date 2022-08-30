import os
import unittest

from astro.databases.snowflake import SnowflakeDatabase, is_valid_snow_identifier
from astro.sql.table import Metadata, Table
from tests.sql.operators import utils as test_utils

# Import Operator
from astro.databases import create_database


class TestSnowflakeMerge(unittest.TestCase):
    def setUp(self):
        self.target_table_name = test_utils.get_table_name("merge_test_1")
        self.target_table = Table(
            name=self.target_table_name,
            metadata=Metadata(
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            ),
            conn_id="snowflake_conn",
        )
        self.source_table_name = test_utils.get_table_name("merge_test_2")
        self.source_table = Table(
            name=self.source_table_name,
            metadata=Metadata(
                database=os.getenv("SNOWFLAKE_DATABASE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            ),
            conn_id="snowflake_conn",
        )
        self.snowflake_db: SnowflakeDatabase = create_database(conn_id="snowflake_conn")

        self.target_table_full_name = self.snowflake_db.get_table_qualified_name(
            self.target_table
        )
        self.source_table_full_name = self.snowflake_db.get_table_qualified_name(
            self.source_table
        )

    def test_merge_func(self):
        sql, parameters = self.snowflake_db._build_merge_sql(
            if_conflicts="update",
            source_table=self.source_table,
            target_table=self.target_table,
            source_to_target_columns_map={"sell": "sell"},
            target_conflict_columns=["sell"],
        )

        assert (
            sql
            == "merge into IDENTIFIER(:target_table) using IDENTIFIER(:source_table) "
            "on Identifier(:merge_clause_target_0)=Identifier(:merge_clause_source_0) "
            f"when matched then UPDATE SET {self.target_table_name}.sell={self.source_table_name}.sell "
            f"when not matched then insert({self.target_table_name}.sell) values ({self.source_table_name}.sell)"
        )
        assert parameters == {
            "merge_clause_target_0": f"{self.target_table_name}.sell",
            "merge_clause_source_0": f"{self.source_table_name}.sell",
            "target_table": self.target_table_full_name,
            "source_table": self.source_table_full_name,
        }

    def test_merge_func_multiple_clause(self):
        sql, parameters = self.snowflake_db._build_merge_sql(
            source_table=self.source_table,
            target_table=self.target_table,
            source_to_target_columns_map={
                "list": "list",
                "sell": "sell",
                "age": "taxes",
            },
            target_conflict_columns=["list", "sell"],
            if_conflicts="update",
        )

        assert (
            sql
            == "merge into IDENTIFIER(:target_table) using IDENTIFIER(:source_table) "
            "on Identifier(:merge_clause_target_0)=Identifier(:merge_clause_source_0) AND "
            "Identifier(:merge_clause_target_1)=Identifier(:merge_clause_source_1) "
            f"when matched then UPDATE SET {self.target_table_name}.list={self.source_table_name}.list,"
            f"{self.target_table_name}.sell={self.source_table_name}.sell,"
            f"{self.target_table_name}.taxes={self.source_table_name}.age "
            f"when not matched then "
            f"insert({self.target_table_name}.list,{self.target_table_name}.sell,{self.target_table_name}.taxes) "
            f"values ({self.source_table_name}.list,{self.source_table_name}.sell,{self.source_table_name}.age)"
        )
        assert parameters == {
            "merge_clause_source_0": f"{self.source_table_name}.list",
            "merge_clause_source_1": f"{self.source_table_name}.sell",
            "merge_clause_target_0": f"{self.target_table_name}.list",
            "merge_clause_target_1": f"{self.target_table_name}.sell",
            "target_table": self.target_table_full_name,
            "source_table": self.source_table_full_name,
        }

    def test_merge_fun_ignore(self):
        sql, parameters = self.snowflake_db._build_merge_sql(
            source_table=self.source_table,
            target_table=self.target_table,
            source_to_target_columns_map={"sell": "sell"},
            target_conflict_columns=["sell"],
            if_conflicts="ignore",
        )

        assert (
            sql
            == "merge into IDENTIFIER(:target_table) using IDENTIFIER(:source_table) "
            "on Identifier(:merge_clause_target_0)=Identifier(:merge_clause_source_0) "
            f"when not matched then insert({self.target_table_name}.sell) values ({self.source_table_name}.sell)"
        )
        assert parameters == {
            "merge_clause_target_0": f"{self.target_table_name}.sell",
            "merge_clause_source_0": f"{self.source_table_name}.sell",
            "target_table": self.target_table_full_name,
            "source_table": self.source_table_full_name,
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
