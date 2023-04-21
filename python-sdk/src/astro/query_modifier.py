from __future__ import annotations

from airflow.exceptions import AirflowException
from sqlalchemy.sql import ClauseElement


class QueryModifier:
    """
    A series of options that will allow users to determine session variables.
    These session variables will relate to queries
    that can happen before or after the query is run.
    """

    def __init__(self, pre_queries: list[str] | None = None, post_queries: list[str] | None = None):
        self.pre_queries = pre_queries or []
        self.post_queries = post_queries or []

    def merge_pre_and_post_queries(self, sql: str | ClauseElement):
        if isinstance(sql, str):
            return ";".join(self.pre_queries + [sql] + self.post_queries)
        elif isinstance(sql, ClauseElement):
            if self.pre_queries or self.post_queries:
                raise AirflowException(
                    "pre-and-post queries are currently only supported for "
                    "string SQL statements."
                    "Please turn your SQL statement into a text statement."
                )
            else:
                return sql
