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
import inspect
from builtins import NotImplementedError
from typing import Callable, Dict, Iterable, Mapping, Optional, Union

import pandas as pd
from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.hooks.base import BaseHook
from airflow.models import DagRun, TaskInstance
from airflow.utils.db import provide_session
from sqlalchemy import create_engine, text
from sqlalchemy.sql.functions import Function

from astro.sql.table import Table, TempTable, create_table_name
from astro.utils import get_hook, postgres_transform, snowflake_transform
from astro.utils.load_dataframe import move_dataframe_to_sql
from astro.utils.schema_util import create_schema_query, get_schema, schema_exists
from astro.utils.table_handler import TableHandler


class SqlDecoratedOperator(DecoratedOperator, TableHandler):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        autocommit: bool = False,
        parameters: dict = None,
        handler: Function = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        raw_sql=False,
        sql="",
        **kwargs,
    ):
        """
        :param to_dataframe: This function allows users to pull the current staging table into a pandas dataframe.

            To use this function, please make sure that your decorated function has a parameter called ``input_df``. This
        parameter will be a pandas.Dataframe that you can modify as needed. Please note that until we implement
        spark and dask dataframes, that you should be mindful as to how large your worker is when pulling large tables.
        :param from_s3: Whether to pull from s3 into current database.

            When set to true, please include a parameter named ``s3_path`` in your TaskFlow function. When calling this
        task, you can specify any s3:// path and Airflow will
        automatically pull that file into your database using Panda's automatic data typing functionality.
        :param from_csv: Whether to pull from a local csv file into current database.

            When set to true, please include a parameter named ``csv_path`` in your TaskFlow function.
        When calling this task, you can specify any local path and Airflow will automatically pull that file into your
        database using Panda's automatic data typing functionality.
        :param kwargs:
        """
        self.raw_sql = raw_sql
        self.autocommit = autocommit
        self.parameters = parameters
        self.handler = handler
        self.kwargs = kwargs or {}
        self.sql = sql
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[Table] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None

        self.database = self.op_kwargs.pop("database", database)
        self.conn_id = self.op_kwargs.pop("conn_id", conn_id)
        self.schema = self.op_kwargs.pop("schema", schema)
        self.warehouse = self.op_kwargs.pop("warehouse", warehouse)
        self.role = self.op_kwargs.pop("role", role)

        super().__init__(
            **kwargs,
        )

    @property
    def conn_type(self):
        return BaseHook.get_connection(self.conn_id).conn_type

    @property
    def hook(self):
        return get_hook(
            conn_id=self.conn_id,
            database=self.database,
            role=self.role,
            schema=self.schema,
            warehouse=self.warehouse,
        )

    def execute(self, context: Dict):

        if not isinstance(self.sql, str):
            cursor = self._run_sql(self.sql, self.parameters)
            if self.handler is not None:
                return self.handler(cursor)
            return cursor

        self.output_schema = self.schema or get_schema()
        self._set_variables_from_first_table()

        conn = BaseHook.get_connection(self.conn_id)

        self.schema = self.schema or get_schema()
        self.user = conn.login
        self.run_id = context.get("run_id")

        self.convert_op_arg_dataframes()
        self.convert_op_kwarg_dataframes()
        self.read_sql()
        self.handle_params(context)
        context = self._add_templates_to_context(context)
        if context:
            self.sql = self.render_template(self.sql, context)
        self._process_params()

        output_table_name: str = ""

        if not self.raw_sql:
            # Create a table name for the temp table
            if not schema_exists(
                hook=self.hook, schema=self.schema, conn_type=self.conn_type
            ):
                self._set_schema_if_needed()

            if not self.output_table or type(self.output_table) == TempTable:
                output_table_name = create_table_name(context=context)
                full_output_table_name = self.handle_output_table_schema(
                    # Since there is no output table defined we have to assume default schema
                    output_table_name,
                    schema=get_schema(),
                )
            else:
                output_table_name = self.output_table.table_name
                full_output_table_name = self.handle_output_table_schema(
                    output_table_name, schema=self.output_table.schema
                )

            self._run_sql(
                f"DROP TABLE IF EXISTS {full_output_table_name};", self.parameters
            )
            self.sql = self.create_temporary_table(self.sql, full_output_table_name)
        else:
            # If there's no SQL to run we simply return
            if self.sql == "" or not self.sql:
                return

        query_result = self._run_sql(self.sql, self.parameters)
        # Run execute function of subclassed Operator.

        if self.output_table:
            if type(self.output_table) == TempTable:
                self.output_table = self.output_table.to_table(
                    table_name=output_table_name, schema=self.output_table.schema
                )
            self.log.info(f"returning table {self.output_table}")
            self.populate_output_table()
            return self.output_table

        elif self.raw_sql:
            if self.handler is not None:
                return self.handler(query_result)
            return None
        else:
            self.output_table = Table(
                table_name=output_table_name,
            )
            self.populate_output_table()
            self.log.info(f"returning table {self.output_table}")
            return self.output_table

    def read_sql(self):
        if self.sql == "":
            sql_stuff = self.python_callable(*self.op_args, **self.op_kwargs)
            # If we return two things, assume the second thing is the params
            if len(sql_stuff) == 2:
                self.sql, self.parameters = sql_stuff
            else:
                self.sql = sql_stuff
                self.parameters = {}
        elif self.sql[-4:] == ".sql":
            with open(self.sql) as file:
                self.sql = file.read().replace("\n", " ")

    def handle_params(self, context):
        # Automatically add any kwargs going into the function
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
        self.parameters.update(self.op_kwargs)  # type: ignore

    def handle_output_table_schema(self, output_table_name, schema=None):
        """
        In postgres, we set the schema in the query itself instead of as a query parameter.
        This function adds the necessary {schema}.{table} notation.
        :param output_table_name:
        :param schema: an optional schema if the output_table has a schema set. Defaults to the temp schema
        :return:
        """
        schema = schema or get_schema()
        if self.conn_type == "postgres" and self.schema:
            output_table_name = schema + "." + output_table_name
        elif self.conn_type == "snowflake" and self.schema and "." not in self.sql:
            output_table_name = self.database + "." + schema + "." + output_table_name
        return output_table_name

    def _set_schema_if_needed(self):
        schema_statement = ""
        if self.conn_type == "postgres":
            schema_statement = create_schema_query(
                conn_type=self.conn_type,
                hook=self.hook,
                schema_id=self.schema,
                user=self.user,
            )
        elif self.conn_type == "snowflake":
            schema_statement = create_schema_query(
                conn_type=self.conn_type,
                hook=self.hook,
                schema_id=self.schema,
                user=self.user,
            )
        self._run_sql(schema_statement, {})

    def get_sql_alchemy_engine(self):
        if self.conn_type == "sqlite":
            uri = self.hook.get_uri().replace("///", "////")
            engine = create_engine(uri)
        else:
            engine = self.hook.get_sqlalchemy_engine()
        return engine

    def _run_sql(self, sql, parameters):
        engine = self.get_sql_alchemy_engine()
        conn = engine.connect()
        if isinstance(sql, str):
            return conn.execute(text(sql), parameters)
        else:
            return conn.execute(sql, parameters)

    def create_temporary_table(self, query, output_table_name, schema=None):
        """
        Create a temp table for the current task instance. This table will be overwritten if the DAG is run again as this
        table is only ever meant to be temporary.
        :param query:
        :param table_name
        :return:
        """

        def clean_trailing_semicolon(query):
            query = query.strip()
            if query and query[-1] == ";":
                query = query[:-1]
            return query

        if schema:
            output_table_name = f"{schema}.{output_table_name}"

        if self.conn_type == "sqlite":
            statement = f"CREATE TABLE {output_table_name} AS {clean_trailing_semicolon(query)};"
        else:
            statement = f"CREATE TABLE {output_table_name} AS ({clean_trailing_semicolon(query)});"
        return statement

    @staticmethod
    def create_cte(query, table_name):
        return f"WITH {table_name} AS ({query}) SELECT * FROM {table_name};"

    @staticmethod
    def create_output_csv_path(context):
        ti: TaskInstance = context["ti"]
        dag_run: DagRun = ti.get_dagrun()
        return f"{dag_run.dag_id}_{ti.task_id}_{int(ti.execution_date.timestamp())}.csv"

    def handle_dataframe_func(self, input_table):
        raise NotImplementedError("Need to add dataframe func to class")

    @provide_session
    def pre_execute(self, context, session=None):
        """This hook is triggered right before self.execute() is called."""
        pass

    def post_execute(self, context, result=None):
        """
        This hook is triggered right after self.execute() is called.
        """
        pass

    def _table_exists_in_db(self, conn: str, table_name: str):
        """Override this method to enable sensing db."""
        raise NotImplementedError("Add _table_exists_in_db method to class")

    def _process_params(self):
        if self.conn_type == "snowflake":
            self.parameters = snowflake_transform.process_params(
                parameters=self.parameters
            )

    def _add_templates_to_context(self, context):
        if self.conn_type in ["postgres", "bigquery"]:
            return postgres_transform.add_templates_to_context(self.parameters, context)
        elif self.conn_type in ["snowflake"]:
            return snowflake_transform.add_templates_to_context(
                self.parameters, context
            )
        else:
            return self.default_transform(self.parameters, context)

    def default_transform(self, parameters, context):
        for k, v in parameters.items():
            if type(v) == Table:
                context[k] = v.table_name
            else:
                context[k] = ":" + k
        return context

    def _cleanup(self):
        """Remove DAG's objects from S3 and db."""
        # To-do
        pass

    def convert_op_arg_dataframes(self):
        final_args = []
        for i, arg in enumerate(self.op_args):
            if type(arg) == pd.DataFrame:
                output_table_name = (
                    self.dag_id
                    + "_"
                    + self.task_id
                    + "_"
                    + self.run_id
                    + f"_input_dataframe_{i}"
                )
                move_dataframe_to_sql(
                    output_table_name=output_table_name,
                    df=arg,
                    conn_type=self.conn_type,
                    conn_id=self.conn_id,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                )
                final_args.append(
                    Table(
                        table_name=output_table_name,
                        conn_id=self.conn_id,
                        database=self.database,
                        schema=self.schema,
                        warehouse=self.warehouse,
                    )
                )
            else:
                final_args.append(arg)
            self.op_args = tuple(final_args)

    def convert_op_kwarg_dataframes(self):
        final_kwargs = {}
        for k, v in self.op_kwargs.items():
            if type(v) == pd.DataFrame:
                output_table_name = "_".join(
                    [self.dag_id, self.task_id, self.run_id, "input_dataframe", str(k)]
                )
                move_dataframe_to_sql(
                    output_table_name=output_table_name,
                    df=v,
                    conn_type=self.conn_type,
                    conn_id=self.conn_id,
                    database=self.database,
                    schema=self.schema,
                    warehouse=self.warehouse,
                )
                final_kwargs[k] = output_table_name
            else:
                final_kwargs[k] = v
        self.op_kwargs = final_kwargs


def _transform_task(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    **kwargs,
):
    """
    Python operator decorator. Wraps a function into an Airflow operator.
    Accepts kwargs for operator kwarg. Can be reused in a single DAG.

    :param python_callable: Function to decorate
    :type python_callable: Optional[Callable]
    :param multiple_outputs: if set, function return value will be
        unrolled to multiple XCom values. List/Tuples will unroll to xcom values
        with index as key. Dict will unroll to xcom values with keys as XCom keys.
        Defaults to False.
    :type multiple_outputs: bool
    """
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SqlDecoratedOperator,  # type: ignore
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
    handler: Callable = None,
):
    """
    :param python_callable:
    :param multiple_outputs:
    :param postgres_conn_id: The :ref:`postgres conn id <howto/connection:postgres>`
        reference to a specific postgres database.
    :type postgres_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    @return:
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
    )
