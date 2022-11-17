from __future__ import annotations

from typing import Any

from astro.custom_backend.serializer import deserialize
from astro.sql import LoadFileOperator


def build_load_file_operator_content(
    operator_content: dict[str, Any], operator_file_name: str
) -> dict[str, Any]:
    input_file_content = operator_content.pop("input_file")
    input_file_content["class"] = "File"
    input_file_content.setdefault("is_dataframe", False)
    input_file_content.setdefault("normalize_config", None)
    input_file = deserialize(input_file_content)
    operator_content["input_file"] = input_file
    output_table_content = operator_content.pop("output_table")
    output_table_content["class"] = "Table"
    output_table_content.setdefault("name", operator_file_name)
    output_table_content.setdefault("temp", False)
    output_table = deserialize(output_table_content)
    operator_content["output_table"] = output_table
    return operator_content


def get_load_file_instance(yaml_content: dict[str, Any], operator_file_name: str) -> LoadFileOperator:
    operator_content: dict[str, Any] = build_load_file_operator_content(yaml_content, operator_file_name)
    load_file_operator = LoadFileOperator(**operator_content)
    return load_file_operator
