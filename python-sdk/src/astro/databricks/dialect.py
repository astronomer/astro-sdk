import logging
import re

from databricks import sql
from pyhive.sqlalchemy_hive import HiveDialect, _type_map
from sqlalchemy import types


class DatabricksDialect(HiveDialect):
    name = "databricks"
    driver = "connector"  # databricks-sql-connector
    supports_statement_cache = False  # can this be True?

    @classmethod
    def dbapi(cls):
        return sql

    def create_connect_args(self, url):
        # databricks-sql-connector expects just
        # server_hostname, access_token, and http_path.
        # schema is extracted from the database in the url
        # http_path is passed as a connect arg outside this method
        kwargs = {
            "server_hostname": url.host,
            "access_token": url.password,
            "schema": url.database or "default",
        }
        return [], kwargs

    def get_table_names(self, connection, schema=None, **kw):
        # override to use row[1] in databricks instead of row[0] in hive
        query = "SHOW TABLES"
        if schema:
            query += " IN " + self.identifier_preparer.quote_identifier(schema)
        return [row[1] for row in connection.execute(query)]

    def get_columns(self, connection, table_name, schema=None, **kw):
        # override to get columns properly; the reason is that databricks
        # presents the partition information differently from oss hive
        rows = self._get_table_columns(connection, table_name, schema)
        # Strip whitespace
        rows = [[col.strip() if col else None for col in row] for row in rows]
        # Filter out empty rows and comment
        rows = [row for row in rows if row[0] and row[0] != "# col_name"]
        result = []
        for (col_name, col_type, _comment) in rows:
            # Handle both oss hive and Databricks' hive partition header, respectively
            if col_name in ("# Partition Information", "# Partitioning"):
                break
            # Take out the more detailed type information
            # e.g. 'map<int,int>' -> 'map'
            #      'decimal(10,1)' -> decimal
            col_type = re.search(r"^\w+", col_type).group(0)
            try:
                coltype = _type_map[col_type]
            except KeyError:
                logging.warning("Did not recognize type '{col_type}' of column '{col_name}'")
                coltype = types.NullType

            result.append(
                {
                    "name": col_name,
                    "type": coltype,
                    "nullable": True,
                    "default": None,
                }
            )
        return result
