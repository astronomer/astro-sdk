import inspect
from typing import Callable, Dict, Iterable, Mapping, Optional, Tuple, Union

import pandas as pd
from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.exceptions import AirflowException
from sqlalchemy.sql.functions import Function

from astro.databases import create_database
from astro.sql.table import Table
from astro.utils.table import find_first_table


class TransformOperator(DecoratedOperator):
    """Handles all decorator classes that can return a SQL function"""

    def __init__(
        self,
        conn_id: Optional[str] = None,
        parameters: Optional[dict] = None,
        handler: Optional[Function] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        raw_sql: bool = False,
        sql: str = "",
        **kwargs,
    ):
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        self.output_table: Table = self.op_kwargs.pop("output_table", Table())
        self.handler = self.op_kwargs.pop("handler", handler)
        self.conn_id = self.op_kwargs.pop("conn_id", conn_id)
        self.sql = sql
        self.parameters = parameters or {}
        self.database = (self.op_kwargs.pop("database", database),)
        self.schema = (self.op_kwargs.pop("schema", schema),)
        self.raw_sql = raw_sql
        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):
        first_table = find_first_table(
            op_args=self.op_args,  # type: ignore
            op_kwargs=self.op_kwargs,
            python_callable=self.python_callable,
            parameters=self.parameters or {},  # type: ignore
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
        self.op_args = load_op_arg_dataframes_into_sql(
            conn_id=self.conn_id,
            op_args=self.op_args,  # type: ignore
            target_table=self.output_table.create_similar_table(),
        )
        self.op_kwargs = load_op_kwarg_dataframes_into_sql(
            conn_id=self.conn_id,
            op_kwargs=self.op_kwargs,
            target_table=self.output_table.create_similar_table(),
        )

        # Get SQL from function and render templates in the SQL String
        self.read_sql_from_function()
        self.move_function_params_into_sql_params(context)
        self.translate_jinja_to_sqlalchemy_template(context)
        # if there is no SQL to run we raise an error
        if self.sql == "" or not self.sql:
            raise AirflowException("There's no SQL to run")

        if self.raw_sql:
            result = self.database_impl.run_sql(
                sql_statement=self.sql, parameters=self.parameters
            )
            if self.handler:
                return self.handler(result)
            else:
                return None
        else:
            self.database_impl.create_schema_if_needed(
                self.output_table.metadata.schema
            )
            self.database_impl.drop_table(self.output_table)
            self.database_impl.create_table_from_select_statement(
                statement=self.sql,
                target_table=self.output_table,
                parameters=self.parameters,
            )
            return self.output_table

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


def _transform_task(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    **kwargs,
):
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """

    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=TransformOperator,  # type: ignore
        **kwargs,
    )


def transform_decorator(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    conn_id: str = "",
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    raw_sql: bool = False,
    handler: Optional[Callable] = None,
    **kwargs,
):
    """
    :param python_callable: A reference to an object that is callable
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. Dict will unroll to xcom values with keys as keys.
        Defaults to False.
    :param conn_id: The reference to a specific Database for input and output table.
    :param autocommit: if True, each command is automatically committed. (default value: False)
    :param parameters: The parameters to render the SQL query with.
    :param database: name of database which overwrite defined one in connection
    """
    return _transform_task(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        conn_id=conn_id,
        parameters=parameters,
        database=database,
        schema=schema,
        raw_sql=raw_sql,
        handler=handler,
        **kwargs,
    )


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
