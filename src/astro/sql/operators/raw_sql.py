from typing import Any, Dict

from astro.sql.operators.base import BaseSQLOperator


class RawSQLOperator(BaseSQLOperator):
    """
    Given a SQL statement, (optional) tables and a (optional) function, execute the SQL statement
    and apply the function to the results, returning the result of the function.

    Disclaimer: this could potentially trash the XCom Database, depending on the XCom backend used
    and on the SQL statement/function declared by the user.
    """

    def execute(self, context: Dict) -> Any:
        super().execute(context)

        result = self.database_impl.run_sql(
            sql_statement=self.sql, parameters=self.parameters
        )
        if self.handler:
            return self.handler(result)
        else:
            return None
