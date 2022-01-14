import os
import re
from collections import defaultdict
from typing import Dict

from airflow.decorators.base import get_unique_task_id

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator


def parse_directory(**kwargs):
    files = [
        f for f in os.listdir(os.getcwd()) if os.path.isfile(f) and f.endswith(".sql")
    ]
    file_dict = {}
    lineage_dict = defaultdict(set)
    for filename in files:
        with open(os.path.join(os.getcwd(), filename), "r") as f:
            file_string = f.read()
            x = [
                y[1:-1] for y in re.findall(r"\{[^}]*\}", file_string) if "{{" not in y
            ]
            sql = file_string
            parameters = {y: None for y in x}
            [lineage_dict[y].add(filename.removesuffix(".sql")) for y in x]
            p = ParsedSqlOperator(sql=sql, parameters=parameters, file_name=filename)
            file_dict[filename] = p
    for filename in files:
        current_operator = file_dict[filename]
        for param in current_operator.parameters:
            current_operator.parameters[param] = file_dict[param + ".sql"].output
    ret = []
    for f in file_dict.keys():
        x = f.removesuffix(".sql")
        if lineage_dict[x] == set():
            ret.append(file_dict[f])
    if len(ret) == 1:
        return ret[0]
    else:
        return ret


class ParsedSqlOperator(SqlDecoratoratedOperator):
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
        task_id = get_unique_task_id(file_name)

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
