from builtins import NotImplementedError
from typing import Callable, Dict, Iterable, Mapping, Optional, Union

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.hooks.base import BaseHook
from airflow.models import DagRun, TaskInstance
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.db import provide_session

from astro.utils import postgres_transform, snowflake_transform


class SqlDecoratoratedOperator(DecoratedOperator):
    def __init__(
        self,
        conn_id: Optional[str] = None,
        autocommit: bool = False,
        parameters: dict = None,
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
        self.warehouse = warehouse
        self.kwargs = kwargs or {}
        self.sql = sql
        self.op_kwargs = self.kwargs.get("op_kwargs")

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):
        self.conn_type = BaseHook.get_connection(self.conn_id).conn_type

        if not self.sql:
            sql_stuff = self.python_callable(**self.op_kwargs)
            # If we return two things, assume the second thing is the params
            if len(sql_stuff) == 2:
                self.sql, self.parameters = sql_stuff
            else:
                self.sql = sql_stuff
                self.parameters = {}
        if context:
            self.sql = self.render_template(self.sql, context)
            self.parameters = {
                k: self.render_template(v, context) for k, v in self.parameters.items()  # type: ignore
            }
        self._parse_template()
        output_table_name = None

        if not self.raw_sql:
            # Create a table name for the temp table
            output_table_name = self.kwargs.get("op_kwargs").get(  # type: ignore
                "output_table_name"
            ) or self.create_table_name(context)
            self.sql = self.create_temporary_table(self.sql, output_table_name)

        # Automatically add any kwargs going into the function
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)  # type: ignore

        self.parameters.update(self.op_kwargs)  # type: ignore

        self._process_params()
        self._run_sql()
        # Run execute function of subclassed Operator.
        return output_table_name

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

    def _run_sql(self):

        if self.conn_type == "postgres":
            self.log.info("Executing: %s", self.sql)
            self.hook = PostgresHook(
                postgres_conn_id=self.conn_id, schema=self.database
            )
            self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
            for output in self.hook.conn.notices:
                self.log.info(output)
        elif self.conn_type == "snowflake":
            self.log.info("Executing: %s", self.sql)
            hook = self.get_snow_hook()
            execution_info = hook.run(
                self.sql, autocommit=self.autocommit, parameters=self.parameters
            )
            self.query_ids = hook.query_ids

            if self.do_xcom_push:
                return execution_info

    @staticmethod
    def create_temporary_table(query, table_name):
        """
        Create a temp table for the current task instance. This table will be overwritten if the DAG is run again as this
        table is only ever meant to be temporary.
        :param query:
        :param table_name:
        :return:
        """
        return f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} AS ({query});"

    @staticmethod
    def create_cte(query, table_name):
        return f"WITH {table_name} AS ({query}) SELECT * FROM {table_name};"

    @staticmethod
    def create_table_name(context):
        ti: TaskInstance = context["ti"]
        dag_run: DagRun = ti.get_dagrun()
        return f"{dag_run.dag_id}_{ti.task_id}_{dag_run.id}"

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
        decorated_operator_class=SqlDecoratoratedOperator,
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
