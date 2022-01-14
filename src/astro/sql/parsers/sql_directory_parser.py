import os
import re
from collections import defaultdict
from typing import Dict

from airflow.decorators.base import get_unique_task_id
from airflow.decorators.task_group import task_group
from airflow.exceptions import AirflowException

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator


# @task_group()
def parse_directory(path):
    # raise AirflowException(f"Failed because cwd is {os.listdir(path)}, {os.}")
    files = [
        f
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f)) and f.endswith(".sql")
    ]
    template_dict = {}

    # Parse all of the SQL files in this directory
    for filename in files:
        with open(os.path.join(path, filename), "r") as f:
            file_string = f.read()
            templated_names = [
                y[1:-1] for y in re.findall(r"\{[^}]*\}", file_string) if "{{" not in y
            ]
            sql = file_string
            parameters = {y: None for y in templated_names}
            p = ParsedSqlOperator(sql=sql, parameters=parameters, file_name=filename)
            template_dict[filename.removesuffix(".sql")] = p

    # Add the XComArg to the parameters to create dependency
    for filename in files:
        current_operator = template_dict[filename.removesuffix(".sql")]
        for param in current_operator.parameters:
            current_operator.parameters[param] = template_dict[param].output
            template_dict[param] >> current_operator
    ret = []
    for f in template_dict.values():
        ret.append(f)


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
        self.sql = ""
        self.parameters = parameters
        task_id = get_unique_task_id(file_name.replace(".sql", ""))

        def null_function():
            return sql, parameters

        super().__init__(
            raw_sql=False,
            task_id=task_id,
            op_args=(),
            op_kwargs={},
            parameters=parameters,
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        super().execute(context)
