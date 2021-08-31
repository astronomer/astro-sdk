import os
from typing import Callable, Dict, Iterable, Optional, Union, Mapping

import pandas as pd
import pandas.io.sql as sqlio
import sqlalchemy

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
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database)
        input_df = sqlio.read_sql_query(
            sql=f"SELECT * FROM {input_table}",
            con=self.hook.get_conn()
        )
        self.op_kwargs["input_df"] = input_df
        return self.python_callable(input_df=input_df)

    def _s3_to_db(self, s3_path, conn, table_name):
        """Transfer table from S3 to Postgres database.

        :param conn:
        :type conn:
        :param s3_path:
        :type s3_path:
        """

        def _s3fs_creds():
            """Structure s3fs credentials from Airflow connection.
            s3fs enables pandas to write to s3
            """
            # To-do: clean-up how S3 creds are passed to s3fs
            k, v = os.environ['AIRFLOW_CONN_AWS_DEFAULT'].replace(
                '%2F', '/').replace('aws://', '').replace('@', '').split(':')

            return {'key': k, 'secret': v}

        # Read CSV from S3
        df = pd.read_csv(s3_path, storage_options=_s3fs_creds())

        # Write df to postgres
        self._df_to_postgres(df, conn, table_name)

    def _csv_to_db(self, csv_path, conn, table_name):
        """Override this method to enable transfer from csv to selected database.
        """
        # Create df from local csv
        df = pd.read_csv(csv_path)

        # Write df to postgres
        self._df_to_postgres(df, conn, table_name)

    def _df_to_postgres(self, df, conn, table_name):
        # Generate target db hook
        self.hook_target = PostgresHook(
            postgres_conn_id=conn,
            schema=self.database)

        # Using a sqlalchemy engine because `to_sql` with `hook_target.get_conn()` returns error
        engine = sqlalchemy.create_engine(
            os.environ['AIRFLOW_CONN_POSTGRES_CONN']).connect()

        # CREATE OR REPLACE table in db
        df.to_sql(table_name,
                  con=engine,
                  schema=None,
                  if_exists='replace',
                  method=None)


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
        from_s3: bool = False,
        from_csv: bool = False,
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
        from_s3=from_s3,
        from_csv=from_csv
    )
    pass
