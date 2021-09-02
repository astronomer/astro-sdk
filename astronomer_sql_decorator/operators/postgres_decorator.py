import os
from typing import Callable, Iterable, Mapping, Optional, Union

import pandas as pd
import pandas.io.sql as sqlio
import sqlalchemy
from airflow.configuration import conf
from airflow.decorators.base import task_decorator_factory
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

from astronomer_sql_decorator.operators.sql_decorator import SqlDecoratoratedOperator


def create_sql_engine(postgres_conn_id, database):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema=database)
    engine = hook.get_sqlalchemy_engine()
    engine.url.database = database
    return engine


class _PostgresDecoratedOperator(SqlDecoratoratedOperator, PostgresOperator):
    def __init__(
        self,
        postgres_conn_id: str = "postgres_default",
        to_dataframe: bool = False,
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
            to_dataframe=to_dataframe,
            **kwargs,
        )

    def handle_dataframe_func(self, input_table):
        """
        If a user wants to use the "to_dataframe" option, we give them a dataframe with the full value of the
        most recent generated table. At this time we do not allow multiple inheritance, but that could be an option
        later.
        """
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        input_df = sqlio.read_sql_query(
            sql=f"SELECT * FROM {input_table}", con=self.hook.get_conn()
        )
        self.op_kwargs["input_df"] = input_df
        return self.python_callable(input_df=input_df)

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

    def _s3_to_db(self, s3_path, table_name):
        """Transfer table from S3 to Postgres database.

        :param conn:
        :type conn:
        :param s3_path:
        :type s3_path:
        """

        # Read CSV from S3
        df = pd.read_csv(s3_path, storage_options=self._s3fs_creds())

        # Write df to postgres
        self._df_to_postgres(df, table_name)

    def _csv_to_db(self, csv_path, table_name):
        """Override this method to enable transfer from csv to selected database."""
        df = pd.read_csv(csv_path)
        self._df_to_postgres(df, table_name)

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

    def _df_to_postgres(self, df, table_name):
        # Generate target db hook
        engine = create_sql_engine(self.postgres_conn_id, self.database)
        df.to_sql(
            table_name,
            con=engine,
            schema=None,
            if_exists="replace",
            method=None,
        )


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
    to_dataframe: bool = False,
    from_s3: bool = False,
    from_csv: bool = False,
    to_s3: bool = False,
    to_csv: bool = False,
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
    :param to_dataframe: This function allows users to pull the current staging table into a pandas dataframe. To
        use this function, please make sure that your decorated function has a parameter called ``input_df``. This
        parameter will be a pandas.Dataframe that you can modify as needed. Please note that until we implement
        spark and dask dataframes, that you should be mindful as to how large your worker is when pulling large tables.
    :param from_s3: Whether to pull from s3 into current database. When set to true, please include a parameter named
        ``s3_path`` in your TaskFlow function. When calling this task, you can specify any s3:// path and Airflow will
        automatically pull that file into your database using Panda's automatic data typing functionality.
    :param from_csv: Whether to pull from a local csv file into current database. When set to true,
        please include a parameter named ``csv_path`` in your TaskFlow function. When calling this task, you can
        specify any local path and Airflow will automatically pull that file into your database using Panda's
        automatic data typing functionality.
    @return:
    """
    return _postgres_task(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        postgres_conn_id=postgres_conn_id,
        autocommit=autocommit,
        parameters=parameters,
        database=database,
        to_dataframe=to_dataframe,
        from_s3=from_s3,
        from_csv=from_csv,
        to_s3=to_s3,
        to_csv=to_csv,
    )
