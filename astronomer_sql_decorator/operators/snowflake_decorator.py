from typing import Callable, Iterable, Optional, Union, Mapping

import pandas.io.sql as sqlio
from airflow.decorators.base import task_decorator_factory
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from astronomer_sql_decorator.operators.sql_decorator import SqlDecoratoratedOperator


class _SnowflakeDecoratedOperator(SqlDecoratoratedOperator, SnowflakeOperator):
    def __init__(
            self,
            snowflake_conn_id: str = 'snowflake_default',
            to_dataframe: bool = False,
            to_temp_table: bool = True,
            **kwargs) -> None:
        super().__init__(
            sql="",
            snowflake_conn_id=snowflake_conn_id,
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
        self.hook = SnowflakeHook(snowflake_conn_id=self.snowflake_conn_id,
                                  database=self.database,
                                  warehouse=self.warehouse)
        cursor = self.hook.get_conn().cursor()
        sql = f"SELECT * FROM {input_table}"
        print(sql)
        cursor.execute(f"SELECT * FROM {input_table}")
        input_df = cursor.fetch_pandas_all()
        self.op_kwargs["input_df"] = input_df
        return self.python_callable(input_df=input_df)


def _snowflake_task(
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
        decorated_operator_class=_SnowflakeDecoratedOperator,
        **kwargs,
    )


def snowflake_decorator(
        python_callable: Optional[Callable] = None,
        multiple_outputs: Optional[bool] = None,
        snowflake_conn_id: str = 'snowflake_default',
        warehouse: str = "",
        autocommit: bool = False,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None,
        to_dataframe: bool = False,
        to_temp_table: bool = True,
):
    return _snowflake_task(
        python_callable=python_callable,
        warehouse=warehouse,
        multiple_outputs=multiple_outputs,
        snowflake_conn_id=snowflake_conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        to_dataframe=to_dataframe,
        to_temp_table=to_temp_table,
    )
