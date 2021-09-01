from typing import Callable, Iterable, Optional, Union, Mapping

import pandas.io.sql as sqlio
from airflow.decorators.base import task_decorator_factory
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from astronomer_sql_decorator.operators.sql_decorator import SqlDecoratoratedOperator


class _SnowflakeDecoratedOperator(SqlDecoratoratedOperator, SnowflakeOperator):
    def __init__(
        self,
        snowflake_conn_id: str = "snowflake_default",
        to_dataframe: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(
            sql="",
            snowflake_conn_id=snowflake_conn_id,
            to_dataframe=to_dataframe,
            **kwargs,
        )

    def handle_dataframe_func(self, input_table):
        """
        If a user wants to use the "to_dataframe" option, we give them a dataframe with the full value of the
        most recent generated table. At this time we do not allow multiple inheritance, but that could be an option
        later.
        """
        self.hook = SnowflakeHook(
            snowflake_conn_id=self.snowflake_conn_id,
            database=self.database,
            warehouse=self.warehouse,
        )
        cursor = self.hook.get_conn().cursor()
        sql = f"SELECT * FROM {input_table}"
        print(sql)
        cursor.execute(f"SELECT * FROM {input_table}")
        input_df = cursor.fetch_pandas_all()
        self.op_kwargs["input_df"] = input_df
        return self.python_callable(input_df=input_df)


def _snowflake_task(
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
        decorated_operator_class=_SnowflakeDecoratedOperator,
        **kwargs,
    )


def snowflake_decorator(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    snowflake_conn_id: str = "snowflake_default",
    warehouse: str = "",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    role: Optional[str] = None,
    schema: Optional[str] = None,
    authenticator: Optional[str] = None,
    session_parameters: Optional[dict] = None,
    database: Optional[str] = None,
    to_dataframe: bool = False,
):
    """

    :param python_callable:
    :param multiple_outputs:
    :param autocommit: if True, each command is automatically committed.
        (default value: True)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: dict or iterable
    :param warehouse: name of warehouse (will overwrite any warehouse
        defined in the connection's extra JSON)
    :type warehouse: str
    :param database: name of database (will overwrite database defined
        in connection)
    :type database: str
    :param schema: name of schema (will overwrite schema defined in
        connection)
    :type schema: str
    :param role: name of role (will overwrite any role defined in
        connection's extra JSON)
    :type role: str
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :type authenticator: str
    :param to_dataframe: This function allows users to pull the current staging table into a pandas dataframe. To
        use this function, please make sure that your decorated function has a parameter called ``input_df``. This
        parameter will be a pandas.Dataframe that you can modify as needed. Please note that until we implement
        spark and dask dataframes, that you should be mindful as to how large your worker is when pulling large tables.
    :param snowflake_conn_id:
    :param session_parameters:
    """
    return _snowflake_task(
        python_callable=python_callable,
        warehouse=warehouse,
        multiple_outputs=multiple_outputs,
        snowflake_conn_id=snowflake_conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        role=role,
        schema=schema,
        authenticator=authenticator,
        session_parameters=session_parameters,
        to_dataframe=to_dataframe,
    )
