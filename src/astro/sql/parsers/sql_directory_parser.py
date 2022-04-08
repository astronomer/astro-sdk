import logging
import os
import pathlib
import re
from typing import Dict, List, Optional

import frontmatter
from airflow.decorators.base import get_unique_task_id
from airflow.exceptions import AirflowException
from airflow.models.dag import DagContext
from airflow.models.xcom_arg import XComArg

from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.table import Table, TempTable


def get_paths_for_render(path):
    """
    Generate a path using either the DAG's template_searchpath with a relative path, or with the relative path
    to the DAG file. We first check to see if the path exists inside of any of the template_searchpaths (Which mean that
    template_searchpaths take precedence), and if we can not find a match, we return the path raw.

    :param path:
    :return:
    """
    template_path = DagContext.get_current_dag().template_searchpath
    ret_paths = []
    if template_path:
        for t in template_path:
            try:
                full_path = str(pathlib.Path(t).joinpath(pathlib.Path(path)))
                os.listdir(full_path)
                logging.info("Template_path found. rendering %s", full_path)
                ret_paths.append(full_path)
            except FileNotFoundError:
                logging.info("Could not find template_path %s", full_path)
    logging.info("No template path found, rendering base path %s", path)
    if ret_paths:
        return ret_paths
    return [path]


def get_all_file_names(paths: List[str]):
    all_file_names = []
    for path in paths:
        current_files = [
            f
            for f in os.listdir(path)
            if os.path.isfile(os.path.join(path, f)) and f.endswith(".sql")
        ]
        all_file_names.extend(current_files)
    return all_file_names


def render_single_path(
    path: str,
    template_dict: dict,
    default_params: dict,
    conn_id: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    params: Optional[dict] = None,
    **kwargs,
):
    # Parse all of the SQL files in this directory
    current_files = [
        f
        for f in os.listdir(path)
        if os.path.isfile(os.path.join(path, f)) and f.endswith(".sql")
    ]
    op_kwargs = {}
    for filename in current_files:
        with open(os.path.join(path, filename)) as f:
            front_matter_opts = frontmatter.loads(f.read()).to_dict()
            sql = front_matter_opts.pop("content")
            temp_items = find_templated_fields(sql)
            parameters = {
                k: v for k, v in default_params.copy().items() if k in temp_items
            }
            if front_matter_opts.get("output_table"):
                out_table_dict = front_matter_opts.pop("output_table")
                if out_table_dict.get("table_name"):
                    op_kwargs = {"output_table": Table(**out_table_dict)}
                else:
                    op_kwargs = {"output_table": TempTable(**out_table_dict)}
            operator_kwargs = set_kwargs_with_defaults(
                front_matter_opts, conn_id, database, role, schema, warehouse
            )
            if params:
                operator_kwargs["params"] = params

            p = ParsedSqlOperator(
                sql=sql,
                parameters=parameters,
                file_name=filename,
                op_kwargs=op_kwargs,
                **operator_kwargs,
            )
            template_dict[filename.replace(".sql", "")] = p.output
    return template_dict


def render(
    path: str = "",
    conn_id: Optional[str] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    warehouse: Optional[str] = None,
    role: Optional[str] = None,
    params: Optional[dict] = None,
    **kwargs,
):
    """
    aql.render renders a single sql file or a directory of sql files and turns them into a group of tasks with
    dependencies. The result of this function is a dictionary of tasks that you can reference in downstream tasks.

    e.g.:

    dag = DAG(
        dag_id="example_postgres_render"
        ...
        template_searchpath=dir_path,
    )


    @adf
    def print_results(df: pd.DataFrame):
        print(df.to_string())

    with dag:
        models = aql.render(path="models")
        print_results(models["top_rentals"])

    :param path: an optional string that determines the directory or file we process. By default we will run your
    path against and `template_searchpath` strings you apply at the DAG level (however you are welcome to supply an
    absolute path as well). If you do not supply a path we will assume that the first template_searchpath houses your
    SQL files (though we do recommend you keep them in subdirectories).
    assume
    :param conn_id: connection ID, can also be supplied in SQL frontmatter.
    :param database:  database name, can also be supplied in SQL frontmatter.
    :param schema: schema name, can also be supplied in SQL frontmatter.
    :param warehouse: warehouse name (snowflake only), can also be supplied in SQL frontmatter.
    :param role: role name (snowflake only), can also be supplied in SQL frontmatter.
    :param params: Parameters you want to pass to the sql file using the tradition {{ params.<your param> }} jinja
    context. Made for backwards compatibility with existing Airflow scripts.
    :param kwargs: any kwargs you supply besides the ones mentioned will be passed on to the model rendering context.
    This means that if you have SQL file that inherits a table named `homes`, you can do something like this:

    ```
       with dag:
        loaded_homes = aql.load_file(...)
        models = aql.render(path="models", homes=loaded_homes)
    ```

    :return: returns a dictionary of type <string, xcomarg>, where the key is the name of the SQL file (minus .sql),
    and the value is the resulting model
    """
    paths = get_paths_for_render(path)

    template_dict = kwargs
    default_params = {}
    default_params.update(template_dict)

    all_file_names = get_all_file_names(paths)
    default_params.update({filename.split(".")[0]: None for filename in all_file_names})

    for path in paths:
        template_dict = render_single_path(
            path=path,
            template_dict=template_dict,
            default_params=default_params,
            conn_id=conn_id,
            database=database,
            schema=schema,
            warehouse=warehouse,
            role=role,
            params=params,
            **kwargs,
        )

    # Add the XComArg to the parameters to create dependency
    for filename in all_file_names:
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


class ParsedSqlOperator(SqlDecoratedOperator):
    template_fields = ("parameters",)

    def __init__(
        self,
        sql,
        parameters,
        file_name,
        op_kwargs=None,
        conn_id: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        **kwargs,
    ):
        if op_kwargs is None:
            op_kwargs = {}
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
