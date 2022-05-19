import inspect
from typing import Callable, Dict, Iterable, Mapping, Optional, Union

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from sqlalchemy.sql.functions import Function

from astro.databases import create_database
from astro.sql.table import Table
from astro.utils.dataframe_function_handler import DataframeFunctionHandler
from astro.utils.sql_handler import SQLHandler
from astro.utils.table_handler_new import TableHandler


class TransformOperator(
    SQLHandler, DataframeFunctionHandler, DecoratedOperator, TableHandler
):
    """Handles all decorator classes that can return a SQL function"""

    # todo: Add docstrings
    def __init__(
        self,
        conn_id: Optional[str] = None,
        autocommit: bool = False,
        parameters: Optional[dict] = None,
        handler: Optional[Function] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        raw_sql=False,
        sql="",
        **kwargs,
    ):
        self.kwargs = kwargs or {}
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        self.output_table: Optional[Table] = self.op_kwargs.pop("output_table", None)
        self.handler = self.op_kwargs.pop("handler", None)
        self.sql = sql

        super().__init__(
            sql=sql,
            autocommit=autocommit,
            handler=handler,
            parameters=parameters,
            conn_id=self.op_kwargs.pop("conn_id", conn_id),
            database=self.op_kwargs.pop("database", database),
            schema=self.op_kwargs.pop("schema", schema),
            warehouse=self.op_kwargs.pop("warehouse", warehouse),
            role=self.op_kwargs.pop("role", role),
            raw_sql=raw_sql,
            **kwargs,
        )

    def execute(self, context: Dict):
        self._set_variables_from_first_table()
        self.database_impl = create_database(self.conn_id)

        # Find and load dataframes from op_arg and op_kwarg into Table
        self.create_output_table(self.output_table_name)
        self.load_op_arg_dataframes_into_sql()
        self.load_op_kwarg_dataframes_into_sql()

        # Get SQL from function and render templates in the SQL String
        self.read_sql_from_function()
        self.move_function_params_into_sql_params(context)
        self.template(context)
        self.parameters = self.database_impl.process_sql_parameters(self.parameters)  # type: ignore
        self.handle_schema()

        if self.raw_sql:
            result = self.database_impl.run_sql(
                sql_statement=self.sql, parameters=self.parameters
            )
            if self.handler:
                return self.handler(result)
            else:
                return None
        else:
            self.database_impl.create_table_from_select_statement(
                statement=self.sql,
                target_table=self.output_table,
                parameters=self.parameters,
            )
            return self.output_table

    def create_output_table(self, output_table_name: str) -> Table:
        """
        If the user has not supplied an output table, this function creates one from scratch, otherwise populates
        the output table with necessary metadata.
        :param output_table_name:
        :return:
        """
        if not self.output_table:
            self.output_table = Table(name=output_table_name)
        self.populate_output_table()
        self.log.info("Returning table %s", self.output_table)
        return self.output_table

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
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
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
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        schema=schema,
        warehouse=warehouse,
        raw_sql=raw_sql,
        handler=handler,
        **kwargs,
    )
