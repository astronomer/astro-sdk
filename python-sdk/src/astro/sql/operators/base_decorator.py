from __future__ import annotations

import inspect
from typing import Any, Callable, Sequence, cast

import jinja2
import pandas as pd
from airflow.decorators.base import DecoratedOperator
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.models.xcom_arg import XComArg
from sqlalchemy.sql.functions import Function

from astro.airflow.datasets import kwargs_with_datasets
from astro.databases import create_database
from astro.databases.base import BaseDatabase
from astro.query_modifier import QueryModifier
from astro.sql.operators.upstream_task_mixin import UpstreamTaskMixin
from astro.table import BaseTable, Table
from astro.utils.compat.functools import cached_property
from astro.utils.compat.typing import Context
from astro.utils.table import find_first_table


class BaseSQLDecoratedOperator(UpstreamTaskMixin, DecoratedOperator):
    """Handles all decorator classes that can return a SQL function"""

    template_fields: Sequence[str] = ("sql", "parameters", "op_args", "op_kwargs")
    template_ext: Sequence[str] = (".sql",)

    def __init__(
        self,
        conn_id: str | None = None,
        parameters: dict | None = None,
        handler: Function | None = None,
        database: str | None = None,
        schema: str | None = None,
        response_limit: int = -1,
        response_size: int = -1,
        sql: str = "",
        query_modifier: QueryModifier = QueryModifier(),
        **kwargs: Any,
    ):
        self.kwargs = kwargs or {}
        self.op_kwargs: dict = self.kwargs.get("op_kwargs") or {}
        self.output_table: BaseTable = self.op_kwargs.pop("output_table", Table())
        self.handler = self.op_kwargs.pop("handler", handler)
        self.conn_id = self.op_kwargs.pop("conn_id", conn_id)

        self.sql = sql
        self.parameters = parameters or {}
        self.database = self.op_kwargs.pop("database", database)
        self.schema = self.op_kwargs.pop("schema", schema)
        self.response_limit = self.op_kwargs.pop("response_limit", response_limit)
        self.response_size = self.op_kwargs.pop("response_size", response_size)

        self.op_args: dict[str, Table | pd.DataFrame] = {}
        self.query_modifier = self.op_kwargs.pop("query_modifier", query_modifier)

        # We purposely do NOT render upstream_tasks otherwise we could have a case where a user
        # has 10 dataframes as upstream tasks and it crashes the worker
        upstream_tasks = self.op_kwargs.pop("upstream_tasks", [])
        super().__init__(
            upstream_tasks=upstream_tasks,
            **kwargs_with_datasets(kwargs=kwargs, output_datasets=self.output_table),
        )

    def _resolve_xcom_op_kwargs(self, context: Context) -> None:
        """
        Iterate through self.op_kwargs, resolving any XCom values with the given context.
        Replace those values in-place.

        :param context: The Airflow Context to be used to resolve the op_kwargs.
        """
        # TODO: confirm if it makes sense for us to always replace the op_kwargs or if we should
        # only replace those that are within the decorator signature, by using
        # inspect.signature(self.python_callable).parameters.values()
        kwargs = {}
        for kwarg_name, kwarg_value in self.op_kwargs.items():
            if isinstance(kwarg_value, XComArg):
                kwargs[kwarg_name] = kwarg_value.resolve(context)
            else:
                kwargs[kwarg_name] = kwarg_value
        self.op_kwargs = kwargs

    def _resolve_xcom_op_args(self, context: Context) -> None:
        """
        Iterates through self.op_args, resolving any XCom values with the given context.
        Replace those values in-place.

        :param context: The Airflow Context used to resolve the op_args.
        """
        args = []
        for arg_value in self.op_args:
            if isinstance(arg_value, XComArg):
                item = arg_value.resolve(context)
            else:
                item = arg_value
            args.append(item)
        self.op_args = args  # type: ignore

    @cached_property
    def database_impl(self) -> BaseDatabase:
        return create_database(self.conn_id)

    def _enrich_context(self, context: Context) -> Context:
        """
        Prepare the sql and context for execution.

        Specifically, it will do the following:
        1. Preset database settings
        2. Load dataframes into tables
        3. Render sql as sqlalchemy executable string

        :param context: The Airflow Context which will be extended.

        :return: the enriched context with astro specific information.
        """
        self._resolve_xcom_op_args(context)
        self._resolve_xcom_op_kwargs(context)

        first_table = find_first_table(
            op_args=self.op_args,  # type: ignore
            op_kwargs=self.op_kwargs,
            python_callable=self.python_callable,
            parameters=self.parameters,  # type: ignore
            context=context,
        )
        if first_table:
            self.conn_id = self.conn_id or first_table.conn_id  # type: ignore
            self.database = self.database or first_table.metadata.database  # type: ignore
            self.schema = self.schema or first_table.metadata.schema  # type: ignore
        else:
            if not self.conn_id:
                raise ValueError("You need to provide a table or a connection id")

        # currently, cross database operation is not supported
        if (
            (first_table and self.output_table)
            and (first_table.sql_type and self.output_table.sql_type)
            and (first_table.sql_type != self.output_table.sql_type)
        ):
            raise ValueError("source and target table must belong to the same datasource")

        self.database_impl.table = first_table

        # Find and load dataframes from op_arg and op_kwarg into Table
        self.create_output_table_if_needed()
        self.op_args = self.load_op_arg_dataframes_into_sql(  # type: ignore
            conn_id=self.conn_id, op_args=self.op_args, output_table=self.output_table  # type: ignore
        )
        self.op_kwargs = self.load_op_kwarg_dataframes_into_sql(
            conn_id=self.conn_id,
            op_kwargs=self.op_kwargs,
            output_table=self.output_table,
        )

        # The transform decorator doesn't explicitly pass output_table as a
        # parameter. Hence, it's not covered in templated fields of class Table.
        self.output_table = self.render_template(self.output_table, context)

        # Get SQL from function and render templates in the SQL String
        self.read_sql_from_function()
        self.move_function_params_into_sql_params(context)
        self.translate_jinja_to_sqlalchemy_template(context)

        # if there is no SQL to run we raise an error
        if self.sql == "" or not self.sql:
            raise AirflowException("There's no SQL to run")
        return context

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> BaseOperator | None:
        """Template all attributes listed in template_fields.

        This mutates the attributes in-place and is irreversible.

        :param context: Dict with values to apply on content
        :param jinja_env: Jinja environment
        """
        context = self._enrich_context(context)
        return super().render_template_fields(context, jinja_env)

    def execute(self, context: Context) -> None:
        context = self._enrich_context(context)

        # TODO: remove pushing to XCom once we update the airflow version.
        context["ti"].xcom_push(key="base_sql_query", value=str(self.sql))

    def create_output_table_if_needed(self) -> None:
        """
        If the user has not supplied an output table, this function creates one from scratch, otherwise populates
        the output table with necessary metadata.
        """
        self.output_table.conn_id = self.output_table.conn_id or self.conn_id
        self.output_table = self.database_impl.populate_table_metadata(self.output_table)
        self.log.info("Returning table %s", self.output_table)

    def read_sql_from_function(self) -> None:
        """
        This function runs the provided python function and stores the resulting
        SQL query in the `sql` attribute. We can also store parameters if the user
        provides a dictionary.
        """
        if self.sql == "":
            # Runs the Python Callable which returns a string
            returned_value = self.python_callable(*self.op_args, **self.op_kwargs)
            # If we return two things, assume the second thing is the params
            if len(returned_value) == 2:
                self.sql, self.parameters = returned_value
            else:
                self.sql = returned_value
                self.parameters = self.parameters or {}
        if self.sql.endswith(".sql"):
            with open(self.sql) as file:
                self.sql = file.read().replace("\n", " ")
        self.op_kwargs.pop("sql", None)

    def move_function_params_into_sql_params(self, context: dict) -> None:
        """
        Pulls values from the function op_args and op_kwargs and places them into
        parameters for SQLAlchemy to parse

        :param context: Airflow's Context dictionary used for rendering templates
        """
        if self.op_kwargs:
            self.parameters.update(self.op_kwargs)  # type: ignore
        if self.op_args:
            params = list(inspect.signature(self.python_callable).parameters.keys())
            for i, arg in enumerate(self.op_args):
                self.parameters[params[i]] = arg  # type: ignore
        if context:
            self.parameters = {
                k: self.render_template(v, context) for k, v in self.parameters.items()  # type: ignore
            }

    def translate_jinja_to_sqlalchemy_template(self, context: dict) -> None:
        """
        This function handles all jinja templating to ensure that the SQL statement is ready for
        processing by SQLAlchemy. We use the database object here as different databases will have
        different templating rules.

        When running functions through the `aql.transform` and `aql.render` functions, we need to add
        the parameters given to the SQL statement to the Airflow context dictionary. This is how we can
        then use jinja to render those parameters into the SQL function when users use the {{}} syntax
        (e.g. "SELECT * FROM {{input_table}}").

        With this system we should handle Table objects differently from other variables. Since we will later
        pass the parameter dictionary into SQLAlchemy, the safest (From a security standpoint) default is to use
        a `:variable` syntax. This syntax will ensure that SQLAlchemy treats the value as an unsafe template. With
        Table objects, however, we have to give a raw value or the query will not work. Because of this we recommend
        looking into the documentation of your database and seeing what best practices exist (e.g. Identifier wrappers
        in snowflake).
        """
        # convert Jinja templating to SQLAlchemy SQL templating, safely converting table identifiers
        for k, v in self.parameters.items():
            if isinstance(v, BaseTable):
                (
                    jinja_table_identifier,
                    jinja_table_parameter_value,
                ) = self.database_impl.get_sqlalchemy_template_table_identifier_and_parameter(v, k)
                context[k] = jinja_table_identifier
                self.parameters[k] = jinja_table_parameter_value
            else:
                context[k] = self.database_impl.parameterize_variable(k)

        # Render templating in sql query
        if context:
            self.sql = self.render_template(self.sql, context)

    def get_openlineage_facets_on_complete(self, task_instance):
        """
        Returns the lineage data
        """
        from astro.lineage import (
            BaseFacet,
            DataSourceDatasetFacet,
            OpenlineageDataset,
            OperatorLineage,
            OutputStatisticsOutputDatasetFacet,
            SchemaDatasetFacet,
            SchemaField,
            SourceCodeJobFacet,
            SqlJobFacet,
        )
        from astro.settings import OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE

        input_dataset: list[OpenlineageDataset] = []
        output_dataset: list[OpenlineageDataset] = []

        first_table = find_first_table(
            op_args=self.op_args,  # type: ignore
            op_kwargs=self.op_kwargs,
            python_callable=self.python_callable,
            parameters=self.parameters,  # type: ignore
            context={},
        )

        if first_table.openlineage_emit_temp_table_event() and first_table.conn_id:  # pragma: no cover
            input_dataset = [
                OpenlineageDataset(
                    namespace=first_table.openlineage_dataset_namespace(),
                    name=first_table.openlineage_dataset_name(),
                    facets={
                        "schema": SchemaDatasetFacet(
                            fields=[SchemaField(name=self.schema, type=self.database)]
                        ),
                        "dataSource": DataSourceDatasetFacet(
                            name=first_table.name, uri=first_table.openlineage_dataset_uri()
                        ),
                    },
                )
            ]
        # TODO: remove pushing to XCom once we update the airflow version.
        self.output_table.conn_id = task_instance.xcom_pull(
            task_ids=task_instance.task_id, key="output_table_conn_id"
        )
        if (
            self.output_table.openlineage_emit_temp_table_event() and self.output_table.conn_id
        ):  # pragma: no cover
            output_table_row_count = task_instance.xcom_pull(
                task_ids=task_instance.task_id, key="output_table_row_count"
            )
            output_dataset = [
                OpenlineageDataset(
                    namespace=self.output_table.openlineage_dataset_namespace(),
                    name=self.output_table.openlineage_dataset_name(),
                    facets={
                        "outputStatistics": OutputStatisticsOutputDatasetFacet(
                            rowCount=output_table_row_count
                        ),
                        "dataSource": DataSourceDatasetFacet(
                            name=self.output_table.name, uri=self.output_table.openlineage_dataset_uri()
                        ),
                    },
                )
            ]

        run_facets: dict[str, BaseFacet] = {}

        base_sql_query = task_instance.xcom_pull(task_ids=task_instance.task_id, key="base_sql_query")
        job_facets: dict[str, BaseFacet] = {"sql": SqlJobFacet(query=base_sql_query)}
        source_code = self.get_source_code(task_instance.task.python_callable)
        if OPENLINEAGE_AIRFLOW_DISABLE_SOURCE_CODE and source_code:
            job_facets.update(
                {
                    "sourceCode": SourceCodeJobFacet("python", source_code),
                }
            )

        return OperatorLineage(
            inputs=input_dataset, outputs=output_dataset, run_facets=run_facets, job_facets=job_facets
        )

    def get_source_code(self, py_callable: Callable) -> str | None:
        """Return the source code for the lineage"""
        try:
            return inspect.getsource(py_callable)
        except TypeError:
            # Trying to extract source code of builtin_function_or_method
            return str(py_callable)
        except OSError:
            self.log.warning("Can't get source code facet of Operator {self.operator.task_id}")
            return None

    def load_op_arg_dataframes_into_sql(self, conn_id: str, op_args: tuple, output_table: BaseTable) -> tuple:
        """
        Identify dataframes in op_args and load them to the table.

        :param conn_id: Connection identifier to be used to load content to the target_table
        :param op_args: user-defined decorator's kwargs
        :param output_table: Similar table where the dataframe content will be written to
        :return: New op_args, in which dataframes are replaced by tables
        """
        final_args: list[Table | BaseTable] = []
        database = self.database_impl or create_database(conn_id=conn_id)
        for arg in op_args:
            if isinstance(arg, pd.DataFrame):
                target_table = output_table.create_similar_table()
                database.load_pandas_dataframe_to_table(source_dataframe=arg, target_table=target_table)
                final_args.append(target_table)
            elif isinstance(arg, BaseTable):
                arg = database.populate_table_metadata(arg)
                final_args.append(arg)
            else:
                final_args.append(arg)
        return tuple(final_args)

    def load_op_kwarg_dataframes_into_sql(
        self, conn_id: str, op_kwargs: dict, output_table: BaseTable
    ) -> dict:
        """
        Identify dataframes in op_kwargs and load them to a table.

        :param conn_id: Connection identifier to be used to load content to the target_table
        :param op_kwargs: user-defined decorator's kwargs
        :param output_table: Similar table where the dataframe content will be written to
        :return: New op_kwargs, in which dataframes are replaced by tables
        """
        final_kwargs = {}
        database = self.database_impl or create_database(conn_id=conn_id)
        database.table = output_table
        for key, value in op_kwargs.items():
            if isinstance(value, pd.DataFrame):
                target_table = output_table.create_similar_table()
                df_table = cast(BaseTable, target_table.create_similar_table())
                database.load_pandas_dataframe_to_table(source_dataframe=value, target_table=df_table)
                final_kwargs[key] = df_table
            elif isinstance(value, BaseTable):
                value = database.populate_table_metadata(value)
                final_kwargs[key] = value
            else:
                final_kwargs[key] = value
        return final_kwargs
