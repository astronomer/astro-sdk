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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.db import provide_session
from sqlalchemy.sql.functions import Function

from astro.sql.table import Table, create_table_name
from astro.utils import postgres_transform, snowflake_transform
from astro.utils.load_dataframe import move_dataframe_to_sql
from astro.utils.schema_util import get_schema, set_schema_query


class SqlDecoratoratedOperator(DecoratedOperator):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        autocommit: bool = False,
        parameters: dict = None,
        handler: Function = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
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
        self.conn_id = conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database
        self.schema = schema
        self.handler = handler
        self.warehouse = warehouse
        self.kwargs = kwargs or {}
        self.sql = sql
        self.op_kwargs: Dict = self.kwargs.get("op_kwargs") or {}
        if self.op_kwargs.get("output_table"):
            self.output_table: Optional[Table] = self.op_kwargs.pop("output_table")
        else:
            self.output_table = None

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):
        self._set_variables_from_first_table()

        conn = BaseHook.get_connection(self.conn_id)
        self.conn_type = conn.conn_type  # type: ignore
        self.schema = self.schema or get_schema()
        self.user = conn.login
        self.run_id = context.get("run_id")
        self.convert_op_arg_dataframes()
        self.convert_op_kwarg_dataframes()
        if not self.sql:
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
        if context:
            self.sql = self.render_template(self.sql, context)
            self.parameters = {
                k: self.render_template(v, context) for k, v in self.parameters.items()  # type: ignore
            }
        self._parse_template()
        output_table_name = None

        if not self.raw_sql:
            # Create a table name for the temp table
            self._set_schema_if_needed()

            if not self.output_table:
                output_table_name = create_table_name(
                    context=context, schema_id=self.schema
                )
            else:
                output_table_name = self.output_table.table_name
            self.sql = self.create_temporary_table(self.sql, output_table_name)

        # Automatically add any kwargs going into the function
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)  # type: ignore

        if self.op_args:
            params = list(inspect.signature(self.python_callable).parameters.keys())
            for i, arg in enumerate(self.op_args):
                self.parameters[params[i]] = arg  # type: ignore

        self.parameters.update(self.op_kwargs)  # type: ignore

        self._process_params()
        query_result = self._run_sql(self.sql, self.parameters)
        # Run execute function of subclassed Operator.

        if self.output_table:
            self.log.info(f"returning table {self.output_table}")
            return self.output_table

        elif self.raw_sql:
            return query_result
        else:
            self.output_table = Table(
                table_name=output_table_name,
                conn_id=self.conn_id,
                database=self.database,
                warehouse=self.warehouse,
                schema=get_schema(),
            )
            self.log.info(f"returning table {self.output_table}")
            return self.output_table

    def _set_variables_from_first_table(self):
        """
        When we create our SQL operation, we run with the assumption that the first table given is the "main table".
        This means that a user doesn't need to define default conn_id, database, etc. in the function unless they want
        to create default values.
        """
        first_table: Optional[Table] = None
        if self.op_args:
            table_index = [x for x, t in enumerate(self.op_args) if type(t) == Table]
            if table_index:
                first_table = self.op_args[table_index[0]]
        if not first_table:
            table_kwargs = [
                x
                for x in inspect.signature(self.python_callable).parameters.values()
                if x.annotation == Table and type(self.op_kwargs[x.name]) == Table
            ]
            if table_kwargs:
                first_table = self.op_kwargs[table_kwargs[0].name]
        if first_table:
            self.conn_id = first_table.conn_id or self.conn_id
            self.database = first_table.database or self.database
            self.schema = first_table.schema or self.schema
            self.warehouse = first_table.warehouse or self.warehouse

    def _set_schema_if_needed(self):
        schema_statement = ""
        if self.conn_type == "postgres":
            self.hook = PostgresHook(
                postgres_conn_id=self.conn_id, schema=self.database
            )
            schema_statement = set_schema_query(
                conn_type=self.conn_type,
                hook=self.hook,
                schema_id=self.schema,
                user=self.user,
            )
        elif self.conn_type == "snowflake":
            hook = self.get_snow_hook()
            schema_statement = set_schema_query(
                conn_type=self.conn_type,
                hook=hook,
                schema_id=self.schema,
                user=self.user,
            )
        self._run_sql(schema_statement, {})

    def get_snow_hook(self) -> SnowflakeHook:
        """
        Create and return SnowflakeHook.
        :return: a SnowflakeHook instance.
        :rtype: SnowflakeHook
        """
        return SnowflakeHook(
            snowflake_conn_id=self.conn_id,
            warehouse=self.warehouse,
            database=self.database,
            role=None,
            schema=self.schema,
            authenticator=None,
            session_parameters=None,
        )

    def _run_sql(self, sql, parameters):

        if self.conn_type == "postgres":
            self.log.info("Executing: %s", sql)
            self.hook = PostgresHook(
                postgres_conn_id=self.conn_id, schema=self.database
            )
            results = self.hook.run(
                sql,
                self.autocommit,
                parameters=parameters,
                handler=self.handler,
            )
            for output in self.hook.conn.notices:
                self.log.info(output)

        elif self.conn_type == "snowflake":
            self.log.info("Executing: %s", sql)
            hook = self.get_snow_hook()
            results = hook.run(sql, autocommit=self.autocommit, parameters=parameters)
            self.query_ids = hook.query_ids

        return results

    @staticmethod
    def create_temporary_table(query, output_table_name, schema=None):
        """
        Create a temp table for the current task instance. This table will be overwritten if the DAG is run again as this
        table is only ever meant to be temporary.
        :param query:
        :param table_name:
        :return:
        """
        if schema:
            output_table_name = f"{schema}.{output_table_name}"
        return f"DROP TABLE IF EXISTS {output_table_name}; CREATE TABLE {output_table_name} AS ({query});"

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
        if self.conn_type == "postgres":
            self.parameters = postgres_transform.process_params(
                self.parameters, self.python_callable
            )
        elif self.conn_type == "snowflake":
            self.parameters = snowflake_transform.process_params(self.parameters)

    def _parse_template(self):
        if self.conn_type == "postgres":
            self.sql = postgres_transform.parse_template(self.sql)
        else:
            self.sql = snowflake_transform._parse_template(
                self.sql, self.python_callable
            )

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
        decorated_operator_class=SqlDecoratoratedOperator,  # type: ignore
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
    )
