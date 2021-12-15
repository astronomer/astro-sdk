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


def handle_table(t: Table):
    return t.schema + "." + t.table_name if t.schema else t.table_name


def process_params(parameters):
    return {
        k: (handle_table(v) if type(v) == Table else v) for k, v in parameters.items()
    }


def _parse_template(sql, python_callable):
    param_types = inspect.signature(python_callable).parameters
    sql = sql.replace("{", "%(").replace("}", ")s")
    all_vals = re.findall("%\(.*?\)s", sql)
    mod_vals = {
        f: f"IDENTIFIER({f})"
        if param_types.get(f[2:-2], None)
        and param_types.get(f[2:-2], None).annotation == Table
        else f
        for f in all_vals
    }
    for k, v in mod_vals.items():
        sql = sql.replace(k, v)
    return sql
