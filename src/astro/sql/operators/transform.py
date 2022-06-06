from typing import Dict

from astro.sql.operators.base import BaseSQLOperator


class TransformOperator(BaseSQLOperator):
    """
    Given a SQL statement and (optional) tables, execute the SQL statement and output
    the result into a SQL table.
    """

    def execute(self, context: Dict):
        super().execute(context)

        self.database_impl.create_schema_if_needed(self.output_table.metadata.schema)
        self.database_impl.drop_table(self.output_table)
        self.database_impl.create_table_from_select_statement(
            statement=self.sql,
            target_table=self.output_table,
            parameters=self.parameters,
        )
        return self.output_table
