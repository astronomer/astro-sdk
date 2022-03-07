import os
import re
from typing import Dict, Optional

import frontmatter
from airflow.decorators.base import get_unique_task_id
from airflow.decorators.task_group import task_group
from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg

from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.table import Table, TempTable


def render(
    path,
    conn_id: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    **kwargs,
):
    # raise AirflowException(f"Failed because cwd is {os.listdir(path)}, {os.}")
    files = [
        f
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f)) and f.endswith(".sql")
    ]
    template_dict = kwargs
    op_kwargs = {}
    default_params = {filename.split(".")[0]: None for filename in files}
    default_params.update(template_dict)

    # Parse all of the SQL files in this directory
    for filename in files:
        with open(os.path.join(path, filename)) as f:
            front_matter_opts = frontmatter.loads(f.read()).to_dict()
            sql = front_matter_opts.pop("content")
            temp_items = find_templated_fields(sql)
            parameters = {
                k: v for k, v in default_params.copy().items() if k in temp_items
            }

            if front_matter_opts.get("template_vars"):
                template_variables = front_matter_opts.pop("template_vars")
                parameters.update({v: None for k, v in template_variables.items()})
            if front_matter_opts.get("output_table"):
                out_table_dict = front_matter_opts.pop("output_table")
                if out_table_dict.get("table_name"):
                    op_kwargs = {"output_table": Table(**out_table_dict)}
                else:
                    op_kwargs = {"output_table": TempTable(**out_table_dict)}
            operator_kwargs = set_kwargs_with_defaults(
                front_matter_opts, conn_id, database, role, schema, warehouse
            )

            p = ParsedSqlOperator(
                sql=sql,
                parameters=parameters,
                file_name=filename,
                op_kwargs=op_kwargs,
                **operator_kwargs,
            )
            template_dict[filename.replace(".sql", "")] = p.output

    # Add the XComArg to the parameters to create dependency
    for filename in files:
        current_operator = template_dict[filename.replace(".sql", "")].operator
        for param in current_operator.parameters:
            if not template_dict.get(param):
                raise AirflowException(
                    f"Table {param} does not exist for file {filename}"
                )
            current_operator.parameters[param] = template_dict[param]
            # due to an edge case in XComArg, we need to explicitly set dependencies here
            if type(template_dict[param]) == XComArg:
                template_dict[param].operator >> current_operator
    ret = []
    for f in template_dict.values():
        ret.append(f)
    return template_dict


def set_kwargs_with_defaults(
    opts_without_defaults, conn_id, database, role, schema, warehouse
):
    opts_without_defaults["conn_id"] = opts_without_defaults.get("conn_id", conn_id)
    opts_without_defaults["database"] = opts_without_defaults.get("database", database)
    opts_without_defaults["schema"] = opts_without_defaults.get("schema", schema)
    opts_without_defaults["role"] = opts_without_defaults.get("role", role)
    opts_without_defaults["warehouse"] = opts_without_defaults.get(
        "warehouse", warehouse
    )
    return opts_without_defaults


def find_templated_fields(file_string):
    return [y[2:-2] for y in re.findall(r"\{\{[^}]*\}\}", file_string)]


def wrap_template_variables(sql, template_vars):
    words = sql.split(" ")
    fixed_words = [
        "{" + template_vars.get(w) + "}" if template_vars.get(w) else w for w in words
    ]
    return " ".join(fixed_words)


class ParsedSqlOperator(SqlDecoratedOperator):
    template_fields = ("parameters",)

    def _table_exists_in_db(self, conn: str, table_name: str):
        pass

    def handle_dataframe_func(self, input_table):
        pass

    def __init__(
        self,
        sql,
        parameters,
        file_name,
        op_kwargs={},
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        **kwargs,
    ):
        self.sql = sql
        self.parameters = parameters
        task_id = get_unique_task_id(file_name.replace(".sql", ""))

        def null_function():
            return sql, parameters

        super().__init__(
            conn_id=conn_id,
            database=database,
            warehouse=warehouse,
            schema=schema,
            role=role,
            raw_sql=False,
            task_id=task_id,
            sql=sql,
            op_args=(),
            parameters=parameters,
            python_callable=null_function,
            op_kwargs=op_kwargs,
            **kwargs,
        )

    def set_values(self, table: Table):
        self.conn_id = self.conn_id or table.conn_id  # type: ignore
        self.schema = self.schema or table.schema  # type: ignore
        self.database = self.database or table.database  # type: ignore
        self.warehouse = self.warehouse or table.warehouse  # type: ignore
        self.role = self.role or table.role  # type: ignore

    def execute(self, context: Dict):
        if self.parameters:
            for v in self.parameters.values():
                if type(v) == Table:
                    self.set_values(v)
        return super().execute(context)
