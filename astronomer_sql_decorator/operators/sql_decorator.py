import os
from builtins import NotImplementedError
from typing import Dict

import pandas as pd
import pandas.io.sql as sqlio
from airflow.decorators.base import DecoratedOperator
from airflow.models import DagRun, TaskInstance
from airflow.models.xcom import BaseXCom
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.db import check, provide_session


class SqlDecoratoratedOperator(DecoratedOperator):
    def __init__(
        self,
        raw_sql=False,
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
        self.kwargs = kwargs
        self.op_kwargs = self.kwargs.get("op_kwargs")

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):

        sql_stuff = self.python_callable(**self.op_kwargs)

        # To-do: Type check `sql_stuff`

        # If we return two things, assume the second thing is the params
        if len(sql_stuff) == 2:
            self.sql, self.parameters = sql_stuff
        else:
            self.sql = sql_stuff
            self.parameters = {}

        self._parse_template()
        # Create a table name for the temp table
        ouput_table_name = self.kwargs.get("op_kwargs").get(
            "output_table_name"
        ) or self.create_table_name(context)

        if not self.raw_sql:
            self.sql = self.create_temporary_table(self.sql, ouput_table_name)

        # Automatically add any kwargs going into the function
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)

        self.parameters.update(self.op_kwargs)
        self._process_params()

        # Run execute function of subclassed Operator.
        super().execute(context)
        return ouput_table_name

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
        pass

    def _parse_template(self):
        """Override this method to enable sensing db."""
        raise NotImplementedError("Add _parse_template method to class")

    def _transfer_to_s3(self, conn: str, table_name: str):
        """Override this method to enable write to S3."""
        raise NotImplementedError("Add _transfer_to_s3 method to class")

    def _transfer_from_s3(self, conn: str, table_name: str):
        """Override this method to enable read from to S3."""
        raise NotImplementedError("Add _transfer_from_s3 method to class")

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

    def _cleanup(self):
        """Remove DAG's objects from S3 and db."""
        # To-do
        pass

    def _db_to_s3(self, s3_path: str, table_name: str):
        """Override this method to enable transfer from S3 to selected database."""
        raise NotImplementedError("Add _s3_to_db method to class")

    def _db_to_csv(self, csv_path: str, table_name: str):
        """Override this method to enable transfer from S3 to selected database."""
        raise NotImplementedError("Add _s3_to_db method to class")
