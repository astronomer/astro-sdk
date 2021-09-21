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
from psycopg2.extensions import AsIs


class SqlDecoratoratedOperator(DecoratedOperator):
    def __init__(
        self,
        from_s3=False,
        from_csv=False,
        to_s3=False,
        to_csv=False,
        safe_parameters=[],
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
        self.from_s3 = from_s3
        self.from_csv = from_csv
        self.to_s3 = to_s3
        self.to_csv = to_csv
        self.kwargs = kwargs
        self.safe_parameters = safe_parameters
        self.op_kwargs = self.kwargs.get("op_kwargs")

        super().__init__(
            **kwargs,
        )

    def execute(self, context: Dict):

        if self.from_s3:
            # Load from s3
            self._s3_to_db(
                s3_path=self.op_kwargs.get("s3_path"),
                table_name=self.op_kwargs["input_table"],
            )

        elif self.from_csv:
            # Load from csv
            self._csv_to_db(
                csv_path=self.op_kwargs.get("csv_path"),
                table_name=self.op_kwargs["input_table"],
            )

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

        self.sql = self.create_temporary_table(self.sql, ouput_table_name)

        # Automatically add any kwargs going into the function
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)

        # While normally it is a security anti-pattern to use AsIs in SQL, this value is never user controlled
        # The only way a user could modify this value is if they already own the metadata DB, which would be a much
        # deeper security breach.
        self.parameters.update(self.op_kwargs)
        self.parameters = {
            k: (AsIs(v) if k in self.safe_parameters else v)
            for k, v in self.parameters.items()
        }

        # Run execute function of subclassed Operator.
        super().execute(context)

        if self.to_csv:
            if self.op_kwargs.get("csv_path"):
                csv_path = self.op_kwargs.get("csv_path")
            else:
                csv_path = self.create_output_csv_path(context)

            self._db_to_csv(csv_path=csv_path, table_name=ouput_table_name)
            return csv_path
        elif self.to_s3:
            if self.op_kwargs.get("s3_path"):
                s3_path = self.op_kwargs.get("s3_path")
            else:
                s3_path = self.create_output_csv_path(context)

            self._db_to_s3(s3_path=s3_path, table_name=ouput_table_name)
        else:
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

    def handle_input_table(self):
        """
        For security reasons, we don't want the user having actual control over the input_table variable.
        This is because there is no sql injection safe way that we can allow user input for this variable.
        This function pulls the input table out of the arguments
        :return:
        """
        if not self.input_table:
            if self.op_kwargs.get("input_table"):
                input_table = self.op_kwargs.pop("input_table")
            else:
                op_args_list = list(self.op_args)
                input_table = op_args_list.pop(0)
                self.op_args = tuple(op_args_list)
        else:
            input_table = self.input_table
        return input_table

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

    def _s3_to_db(self, s3_path: str, table_name: str):
        """Override this method to enable transfer from S3 to selected database."""
        raise NotImplementedError("Add _s3_to_db method to class")

    def _csv_to_db(self, csv_path: str, table_name: str):
        """Override this method to enable transfer from csv to selected database."""
        raise NotImplementedError("Add _csv_to_db method to class")

    def _db_to_s3(self, s3_path: str, table_name: str):
        """Override this method to enable transfer from S3 to selected database."""
        raise NotImplementedError("Add _s3_to_db method to class")

    def _db_to_csv(self, csv_path: str, table_name: str):
        """Override this method to enable transfer from S3 to selected database."""
        raise NotImplementedError("Add _s3_to_db method to class")
