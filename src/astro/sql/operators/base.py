import inspect
from typing import Any, Dict, Optional, Tuple, Union

import pandas as pd
from airflow.decorators.base import DecoratedOperator
from airflow.exceptions import AirflowException
from sqlalchemy.sql.functions import Function

from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.sql.table import Table
from astro.utils.table import find_first_table


class BaseSQLOperator(DecoratedOperator):
    """Handles all decorator classes that can return a SQL function"""

    database_impl: BaseDatabase

    def __init__(
        self,
        conn_id: Optional[str] = None,
        parameters: Optional[dict] = None,
        handler: Optional[Function] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        sql: str = "",
        **kwargs: Any,
    ):
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        self.output_table: Table = self.op_kwargs.pop("output_table", Table())
        self.handler = self.op_kwargs.pop("handler", handler)
        self.conn_id = self.op_kwargs.pop("conn_id", conn_id)
        self.sql = sql
        self.parameters = parameters or {}
        self.database = self.op_kwargs.pop("database", database)
        self.schema = self.op_kwargs.pop("schema", schema)
        self.op_args: Dict[str, Union[Table, pd.DataFrame]] = {}
        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict) -> None:
        first_table = find_first_table(
            op_args=self.op_args,  # type: ignore
            op_kwargs=self.op_kwargs,
            python_callable=self.python_callable,
            parameters=self.parameters,  # type: ignore
        )
        if first_table:
            self.conn_id = self.conn_id or first_table.conn_id  # type: ignore
            self.database = self.database or first_table.metadata.database  # type: ignore
            self.schema = self.schema or first_table.metadata.schema  # type: ignore
        else:
            if not self.conn_id:
                raise ValueError("You need to provide a table or a connection id")
        self.database_impl = create_database(self.conn_id)

        # Find and load dataframes from op_arg and op_kwarg into Table
        self.create_output_table_if_needed()
        self.op_args = load_op_arg_dataframes_into_sql(  # type: ignore
            conn_id=self.conn_id,
            op_args=self.op_args,  # type: ignore
            target_table=self.output_table.create_similar_table(),
        )
        self.op_kwargs = load_op_kwarg_dataframes_into_sql(
            conn_id=self.conn_id,
            op_kwargs=self.op_kwargs,
            target_table=self.output_table.create_similar_table(),
        )

        # The transform decorator doesn't explicitly pass output_table as a
        # parameter. Hence, it's not covered in templated fields of class Table.
        self.output_table = self.render_template(self.output_table, context)

        # Get SQL from function and render templates in the SQL String
        self.read_sql_from_function()
        self.move_function_params_into_sql_params(context)
        self.translate_jinja_to_sqlalchemy_template(context)
        # if there is no SQL to run we raise an error
        if self.sql == "" or not self.sql:
            raise AirflowException("There's no SQL to run")

    def create_output_table_if_needed(self) -> None:
        """
        If the user has not supplied an output table, this function creates one from scratch, otherwise populates
        the output table with necessary metadata.
        """
        self.output_table.conn_id = self.output_table.conn_id or self.conn_id
        self.output_table = self.database_impl.populate_table_metadata(
            self.output_table
        )
        self.log.info("Returning table %s", self.output_table)

    def read_sql_from_function(self) -> None:
        """
        This function runs the provided python function and stores the resulting
        SQL query in the `sql` attribute. We can also store parameters if the user
        provides a dictionary.
        """
        if self.sql == "":
            # Runs the Python Callable which returns a string
            returned_value = self.python_callable(*self.op_args, **self.op_kwargs)
            # If we return two things, assume the second thing is the params
            if len(returned_value) == 2:
                self.sql, self.parameters = returned_value
            else:
                self.sql = returned_value
                self.parameters = {}
        elif self.sql.endswith(".sql"):
            with open(self.sql) as file:
                self.sql = file.read().replace("\n", " ")

    def move_function_params_into_sql_params(self, context: Dict) -> None:
        """
        Pulls values from the function op_args and op_kwargs and places them into
        parameters for SQLAlchemy to parse

        :param context: Airflow's Context dictionary used for rendering templates
        """
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)  # type: ignore
        if self.op_args:
            params = list(inspect.signature(self.python_callable).parameters.keys())
            for i, arg in enumerate(self.op_args):
                self.parameters[params[i]] = arg  # type: ignore
        if context:
            self.parameters = {
                k: self.render_template(v, context) for k, v in self.parameters.items()  # type: ignore
            }

    def translate_jinja_to_sqlalchemy_template(self, context: Dict) -> None:
        """
        This function handles all jinja templating to ensure that the SQL statement is ready for
        processing by SQLAlchemy. We use the database object here as different databases will have
        different templating rules.

        When running functions through the `aql.transform` and `aql.render` functions, we need to add
        the parameters given to the SQL statement to the Airflow context dictionary. This is how we can
        then use jinja to render those parameters into the SQL function when users use the {{}} syntax
        (e.g. "SELECT * FROM {{input_table}}").

        With this system we should handle Table objects differently from other variables. Since we will later
        pass the parameter dictionary into SQLAlchemy, the safest (From a security standpoint) default is to use
        a `:variable` syntax. This syntax will ensure that SQLAlchemy treats the value as an unsafe template. With
        Table objects, however, we have to give a raw value or the query will not work. Because of this we recommend
        looking into the documentation of your database and seeing what best practices exist (e.g. Identifier wrappers
        in snowflake).
        """
        # convert Jinja templating to SQLAlchemy SQL templating, safely converting table identifiers
        for k, v in self.parameters.items():
            if isinstance(v, Table):
                (
                    jinja_table_identifier,
                    jinja_table_parameter_value,
                ) = self.database_impl.get_sqlalchemy_template_table_identifier_and_parameter(
                    v, k
                )
                context[k] = jinja_table_identifier
                self.parameters[k] = jinja_table_parameter_value
            else:
                context[k] = ":" + k

        # Render templating in sql query
        if context:
            self.sql = self.render_template(self.sql, context)


def load_op_arg_dataframes_into_sql(
    conn_id: str, op_args: Tuple, target_table: Table
) -> Tuple:
    """
    Identify dataframes in op_args and load them to the table.

    :param conn_id: Connection identifier to be used to load content to the target_table
    :param op_args: user-defined decorator's kwargs
    :param target_table: Table where the dataframe content will be written to
    :return: New op_args, in which dataframes are replaced by tables
    """
    final_args = []
    database = create_database(conn_id=conn_id)
    for arg in op_args:
        if isinstance(arg, pd.DataFrame):
            database.load_pandas_dataframe_to_table(
                source_dataframe=arg, target_table=target_table
            )
            final_args.append(target_table)
        elif isinstance(arg, Table):
            arg = database.populate_table_metadata(arg)
            final_args.append(arg)
        else:
            final_args.append(arg)
    return tuple(final_args)


def load_op_kwarg_dataframes_into_sql(
    conn_id: str, op_kwargs: Dict, target_table: Table
) -> Dict:
    """
    Identify dataframes in op_kwargs and load them to a table.

    :param conn_id: Connection identifier to be used to load content to the target_table
    :param op_kwargs: user-defined decorator's kwargs
    :param target_table: Table where the dataframe content will be written to
    :return: New op_kwargs, in which dataframes are replaced by tables
    """
    final_kwargs = {}
    database = create_database(conn_id=conn_id)
    for key, value in op_kwargs.items():
        if isinstance(value, pd.DataFrame):
            df_table = target_table.create_similar_table()
            database.load_pandas_dataframe_to_table(
                source_dataframe=value, target_table=df_table
            )
            final_kwargs[key] = df_table
        elif isinstance(value, Table):
            value = database.populate_table_metadata(value)
            final_kwargs[key] = value
        else:
            final_kwargs[key] = value
    return final_kwargs
