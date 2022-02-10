"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
import inspect
import re

from astro.sql.table import Table


def _handle_table(t: Table):
    """
    returns the fully qualified name ("DATABASE"."SCHEMA"."TABLE").
    This will allow snowflake users to do cross schema and cross database queries
    :param t:
    :return:
    """

    return t.database + "." + t.schema + "." + t.table_name


def process_params(parameters):
    return parameters
    # return {
    #     k: (_handle_table(v) if type(v) == Table else v) for k, v in parameters.items()
    # }


def _parse_template(sql, python_callable, parameters):
    for k, v in parameters.items():
        param_string = "{{" + k + "}}"
        if type(v) == Table:
            sql = sql.replace(param_string, "IDENTIFIER('" + v.table_name + "')")
        else:
            sql = sql.replace(param_string, ":" + k)
    return sql
