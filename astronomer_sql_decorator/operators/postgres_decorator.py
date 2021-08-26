from typing import Callable, Dict, Iterable, Optional, Union, Mapping

import pandas.io.sql as sqlio
from airflow.decorators.base import task_decorator_factory
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extensions import AsIs

from astronomer_sql_decorator.operators.sql_decorator import SqlDecoratoratedOperator


class _PostgresDecoratedOperator(SqlDecoratoratedOperator, PostgresOperator):
    def __init__(
            self,
            postgres_conn_id: str = 'postgres_default',
            to_dataframe: bool = False,
            to_temp_table: bool = True,
            **kwargs) -> None:
        super().__init__(
            sql="",
            postgres_conn_id=postgres_conn_id,
            to_dataframe=to_dataframe,
            to_temp_table=to_temp_table,
            **kwargs,
        )

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
