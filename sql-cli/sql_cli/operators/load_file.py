from __future__ import annotations

from typing import Any

from astro.custom_backend.serializer import deserialize
from astro.sql import LoadFileOperator


def get_load_file_instance(operator_content: dict[str, Any], operator_file_name: str) -> LoadFileOperator:
    input_file_content = operator_content.pop("input_file")
    input_file_content["class"] = "File"
    if "is_dataframe" not in input_file_content:
        input_file_content["is_dataframe"] = False
    if "normalize_config" not in input_file_content:
        input_file_content["normalize_config"] = None
    input_file = deserialize(input_file_content)
    operator_content["input_file"] = input_file
    output_table_content = operator_content.pop("output_table")
    output_table_content["class"] = "Table"
    if "name" not in output_table_content:
        output_table_content["name"] = operator_file_name
    if "temp" not in output_table_content:
        output_table_content["temp"] = False
    output_table = deserialize(output_table_content)
    operator_content["output_table"] = output_table
    load_file_operator = LoadFileOperator(**operator_content)
    return load_file_operator
