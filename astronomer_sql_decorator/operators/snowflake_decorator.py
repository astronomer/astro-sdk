import inspect
import os
import re
from typing import Callable, Iterable, Mapping, Optional, Union

import pandas as pd
import pandas.io.sql as sqlio
from airflow.decorators.base import task_decorator_factory
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from astronomer_sql_decorator.operators.sql_decorator import SqlDecoratoratedOperator
from astronomer_sql_decorator.sql.types import Table


def _wrap_identifiers(sql, identifier_params):
    all_vals = re.findall("%\(.*?\)s", sql)
    mod_vals = {
        f: f"IDENTIFIER({f})" if f[2:-2] in identifier_params else f for f in all_vals
    }
    for k, v in mod_vals.items():
        sql = sql.replace(k, v)
    return sql


class _SnowflakeDecoratedOperator(SqlDecoratoratedOperator, SnowflakeOperator):
    def __init__(
        self,
        snowflake_conn_id: str = "snowflake_default",
        **kwargs,
    ) -> None:
        super().__init__(
            sql="",
            snowflake_conn_id=snowflake_conn_id,
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

    def _s3fs_creds(self):
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        """

        # To-do: clean-up how S3 creds are passed to s3fs
        k, v = (
            os.environ["AIRFLOW_CONN_AWS_DEFAULT"]
            .replace("%2F", "/")
            .replace("aws://", "")
            .replace("@", "")
            .split(":")
        )

        return {"key": k, "secret": v}

    def _parse_template(self):
        param_types = inspect.signature(self.python_callable).parameters
        self.sql = self.sql.replace("{", "%(").replace("}", ")s")
        all_vals = re.findall("%\(.*?\)s", self.sql)
        mod_vals = {
            f: f"IDENTIFIER({f})"
            if param_types.get(f[2:-2], None)
            and param_types.get(f[2:-2], None).annotation == Table
            else f
            for f in all_vals
        }
        for k, v in mod_vals.items():
            self.sql = self.sql.replace(k, v)


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
    warehouse: Optional[str] = "",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    role: Optional[str] = None,
    schema: Optional[str] = None,
    authenticator: Optional[str] = None,
    session_parameters: Optional[dict] = None,
    database: Optional[str] = None,
    raw_sql: bool = False,
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
        raw_sql=raw_sql,
    )


def snowflake_append_func(
    main_table, columns, casted_columns, append_table, snowflake_conn_id
):
    def wrap_identifier(inp):
        return "Identifier(%(" + inp + ")s)"

    if columns or casted_columns:
        statement = (
            "INSERT INTO Identifier(%(main_table)s) ({main_cols}{sep}{main_casted_cols})"
            "(SELECT {fields}{sep}{casted_fields} FROM Identifier(%(append_table)s))"
        )
    else:
        statement = "INSERT INTO Identifier(%(main_table)s) (SELECT * FROM Identifier(%(append_table)s))"

    col_dict = {
        f"col{i}": x for i, x in enumerate(columns)
    }  # prevent directly putting user input into query

    casted_col_dict = {f"casted_col{i}": x for i, x in enumerate(casted_columns.keys())}

    statement = (
        statement.replace("{sep}", ",")
        if columns and casted_columns
        else statement.replace("{sep}", "")
    )
    statement = statement.replace(
        "{main_cols}", ",".join([c for c in columns])
    )  # Please note that we are not wrapping these in Identifier due to a snowflake bug. Must fix before public release!
    statement = statement.replace(
        "{main_casted_cols}", ",".join([c for c in casted_columns.keys()])
    )  # Please note that we are not wrapping these in Identifier due to a snowflake bug. Must fix before public release!
    statement = statement.replace(
        "{fields}", ",".join([wrap_identifier(c) for c in col_dict.keys()])
    )
    statement = statement.replace(
        "{casted_fields}",
        ",".join(
            [
                "CAST(" + wrap_identifier(c) + " AS " + casted_columns[v] + ")"
                for c, v in casted_col_dict.items()
            ]
        ),
    )
    statement = statement.replace("{", "%(").replace("}", ")s")
    statement = _wrap_identifiers(
        sql=statement,
        identifier_params=[main_table, columns, casted_columns.keys(), append_table],
    )
    col_dict.update(casted_col_dict)
    return statement, col_dict
