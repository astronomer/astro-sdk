import inspect
from typing import Callable, Iterable, Mapping, Optional, Union

import pandas as pd
import pandas.io.sql as sqlio
from airflow.configuration import conf
from airflow.decorators.base import task_decorator_factory
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from psycopg2.extensions import AsIs

from astronomer_sql_decorator.operators.sql_decorator import SqlDecoratoratedOperator
from astronomer_sql_decorator.sql.types import Table


def create_sql_engine(postgres_conn_id, database):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema=database)
    engine = hook.get_sqlalchemy_engine()
    engine.url.database = database
    return engine


class _PostgresDecoratedOperator(SqlDecoratoratedOperator, PostgresOperator):
    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        **kwargs,
    ) -> None:
        """
        :param postgres_conn_id: the connection string that links to a Postgres connection in your Airflow database
        :param to_dataframe:
        :param kwargs:
        """

        super().__init__(
            sql="",
            postgres_conn_id=postgres_conn_id,
            **kwargs,
        )

    def _parse_template(self):
        self.sql = self.sql.replace("{", "%(").replace("}", ")s")

    def _s3fs_creds(self):
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        """

        k, v = (
            conf.get("sql_decorator", "conn_aws_default")
            .replace("%2F", "/")
            .replace("aws://", "")
            .replace("@", "")
            .split(":")
        )

        return {"key": k, "secret": v}

    def _db_to_s3(self, s3_path, table_name):
        """Transfer Postgres database to s3.

        :param s3_path:
        :type s3_path:
        """

        hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        df = sqlio.read_sql_query(f"SELECT * FROM {table_name}", con=hook.get_conn())
        df.to_csv(s3_path, storage_options=self._s3fs_creds())

    def _db_to_csv(self, csv_path: str, table_name: str):
        df = sqlio.read_sql_query(
            con=self.hook.get_conn(), sql=f"SELECT * FROM {table_name}"
        )
        df.to_csv(csv_path)

    def _process_params(self):
        param_types = inspect.signature(self.python_callable).parameters
        self.parameters = {
            k: (AsIs(v) if param_types.get(k).annotation == Table else v)
            for k, v in self.parameters.items()
        }


def _postgres_task(
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
        decorated_operator_class=_PostgresDecoratedOperator,
        **kwargs,
    )


def postgres_decorator(
    python_callable: Optional[Callable] = None,
    multiple_outputs: Optional[bool] = None,
    postgres_conn_id: str = "postgres_default",
    autocommit: bool = False,
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    to_s3: bool = False,
    to_csv: bool = False,
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
    return _postgres_task(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        postgres_conn_id=postgres_conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        to_s3=to_s3,
        to_csv=to_csv,
        raw_sql=raw_sql,
    )


def postgres_append_func(main_table, columns, casted_columns, append_table, conn_id):
    from psycopg2 import sql

    if columns or casted_columns:
        statement = "INSERT INTO {main_table} ({main_cols}{sep}{main_casted_cols})(SELECT {fields}{sep}{casted_fields} FROM {append_table})"
    else:
        statement = "INSERT INTO {main_table} (SELECT * FROM {append_table})"

    column_names = [sql.Identifier(c) for c in columns]
    casted_column_names = [sql.Identifier(k) for k in casted_columns.keys()]
    fields = [sql.Identifier(append_table, c) for c in columns]
    casted_fields = [
        sql.SQL("CAST({k} AS {v})").format(k=sql.Identifier(k), v=sql.SQL(v))
        for k, v in casted_columns.items()
    ]

    query = sql.SQL(statement).format(
        main_cols=sql.SQL(",").join(column_names),
        main_casted_cols=sql.SQL(",").join(casted_column_names),
        main_table=sql.Identifier(main_table),
        fields=sql.SQL(",").join(fields),
        sep=sql.SQL(",") if columns and casted_columns else sql.SQL(""),
        casted_fields=sql.SQL(",").join(casted_fields),
        append_table=sql.Identifier(append_table),
    )

    hook = PostgresHook(postgres_conn_id=conn_id)
    return query.as_string(hook.get_conn())
