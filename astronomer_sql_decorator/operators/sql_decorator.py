from typing import Callable, Dict, Iterable, Optional, Union, Mapping

import pandas.io.sql as sqlio
from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.decorators import apply_defaults
from psycopg2.extensions import AsIs
from airflow.models import TaskInstance, DagRun
import base64


def create_temporary_table(query, table_name):
    """
    Create a temp table for the current task instance. This table will be overwritten if the DAG is run again as this
    table is only ever meant to be temporary.
    :param query:
    :param table_name:
    :return:
    """
    return f"DROP TABLE IF EXISTS {table_name}; CREATE TABLE {table_name} AS ({query});"


def create_cte(query, table_name):
    return f"WITH {table_name} AS ({query}) SELECT * FROM {table_name};"


def create_table_name(context):
    ti: TaskInstance = context['ti']
    dag_run: DagRun = ti.get_dagrun()
    return f"{dag_run.dag_id}_{int(ti.execution_date.timestamp())}_{ti.task_id}"


class _PostgresDecoratedOperator(DecoratedOperator, PostgresOperator):
    @apply_defaults
    def __init__(
            self,
            postgres_conn_id: str = 'postgres_default',
            to_dataframe: bool = False,
            to_temp_table: bool = True,
            **kwargs) -> None:
        self.to_dataframe = to_dataframe
        self.input_table = None
        self.to_temp_table = to_temp_table
        if to_dataframe:
            if kwargs["op_kwargs"].get("input_table"):
                self.input_table = kwargs["op_kwargs"].pop("input_table")
                kwargs["op_kwargs"]["input_df"] = None

        super().__init__(
            sql="",
            postgres_conn_id=postgres_conn_id,
            **kwargs,
        )

    def execute(self, context: Dict):
        input_table = self.handle_input_table()
        if self.to_dataframe:
            return self.handle_dataframe_func(input_table=input_table)
        else:
            sql_stuff = self.python_callable(input_table=self.input_table)

        # If we return two things, assume the second thing is the params
        if len(sql_stuff) == 2:
            self.sql, self.parameters = sql_stuff
        else:
            self.sql = sql_stuff
            self.parameters = {}

        #Create a table name for the temp table
        ouput_table_name = create_table_name(context)
        self.sql = create_temporary_table(self.sql, ouput_table_name)

        # Automatically add any kwargs going into the function
        if self.op_kwargs:
            self.parameters.extend(self.op_kwargs)

        # While normally it is a security anti-pattern to use AsIs in SQL, this value is never user controlled
        # The only way a user could modify this value is if they already own the metadata DB, which would be a much
        # deeper security breach.
        self.parameters['input_table'] = AsIs(input_table)

        super().execute(context)

        if self.to_temp_table:
            return ouput_table_name

    def handle_input_table(self):
        """
        For security reasons, we don't want the user having actual control over the input_table variable.
        This is because there is no sql injection safe way that we can allow user input for this variable.
        This function pulls the input table out of the arguments
        :return:
        """
        if not self.input_table:
            if self.op_kwargs.get('input_table'):
                input_table = self.op_kwargs.pop('input_table')
            else:
                op_args_list = list(self.op_args)
                input_table = op_args_list.pop(0)
                self.op_args = tuple(op_args_list)
        else:
            input_table = self.input_table
        return input_table

    def handle_dataframe_func(self, input_table):
        """
        If a user wants to use the "to_dataframe" option, we give them a dataframe with the full value of the
        most recent generated table. At this time we do not allow multiple inheritance, but that could be an option
        later.
        """
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id, schema=self.database)
        input_df = sqlio.read_sql_query(
            sql=f"SELECT * FROM {input_table}",
            con=self.hook.get_conn()
        )
        self.op_kwargs["input_df"] = input_df
        return self.python_callable(input_df=input_df)


def _postgres_task(
        python_callable: Optional[Callable] = None, multiple_outputs: Optional[bool] = None, **kwargs
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
        decorated_operator_class=_PostgresDecoratedOperator,
        **kwargs,
    )


def postgres_decorator(
        python_callable: Optional[Callable] = None,
        multiple_outputs: Optional[bool] = None,
        postgres_conn_id: str = 'postgres_default',
        autocommit: bool = False,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None,
        to_dataframe: bool = False,
        to_temp_table: bool = True,
):
    return _postgres_task(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        postgres_conn_id=postgres_conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        to_dataframe=to_dataframe,
        to_temp_table=to_temp_table,
    )
    pass
