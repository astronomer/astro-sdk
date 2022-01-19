import os
import re
from collections import defaultdict
from typing import Dict

import frontmatter
from airflow.decorators.base import get_unique_task_id
from airflow.decorators.task_group import task_group
from airflow.exceptions import AirflowException
from airflow.models.xcom_arg import XComArg

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator


@task_group()
def render(path, **kwargs):
    # raise AirflowException(f"Failed because cwd is {os.listdir(path)}, {os.}")
    files = [
        f
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f)) and f.endswith(".sql")
    ]
    template_dict = kwargs

    # Parse all of the SQL files in this directory
    for filename in files:
        with open(os.path.join(path, filename), "r") as f:
            front_matter_opts = frontmatter.loads(f.read()).to_dict()
            sql = front_matter_opts.pop("content")
            templated_names = find_templated_fields(sql)
            parameters = {y: None for y in templated_names}
            if front_matter_opts.get("template_vars"):
                template_variables = front_matter_opts.pop("template_vars")
                sql = wrap_template_variables(sql, template_variables)
                parameters.update({v: None for k, v in template_variables.items()})

            p = ParsedSqlOperator(
                sql=sql, parameters=parameters, file_name=filename, **front_matter_opts
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


def find_templated_fields(file_string):
    return [y[1:-1] for y in re.findall(r"\{[^}]*\}", file_string) if "{{" not in y]


def wrap_template_variables(sql, template_vars):
    words = sql.split(" ")
    fixed_words = [
        "{" + template_vars.get(w) + "}" if template_vars.get(w) else w for w in words
    ]
    return " ".join(fixed_words)


class ParsedSqlOperator(SqlDecoratoratedOperator):
    template_fields = ("sql", "parameters")

    def _table_exists_in_db(self, conn: str, table_name: str):
        pass

    def handle_dataframe_func(self, input_table):
        pass

    def __init__(
        self,
        sql,
        parameters,
        file_name,
        **kwargs,
    ):
        self.sql = sql
        self.parameters = parameters
        task_id = get_unique_task_id(file_name.replace(".sql", ""))

        def null_function():
            return sql, parameters

        super().__init__(
            raw_sql=False,
            task_id=task_id,
            sql=sql,
            op_args=(),
            op_kwargs={},
            parameters=parameters,
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        super().execute(context)
