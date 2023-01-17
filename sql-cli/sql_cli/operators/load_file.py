from __future__ import annotations

from typing import Any

from astro.custom_backend.serializer import deserialize
from astro.sql import LoadFileOperator


def build_load_file_operator_content(
    operator_content: dict[str, Any],
    operator_file_name: str,
) -> dict[str, Any]:
    """
    Deserialize the File and Table classes.

    :param operator_content: The dictionary of the operator content to be deserialized.
    :param operator_file_name: The file name of the operator.

    :returns: the operator content deserialized.
    """
    input_file_content = operator_content.pop("input_file")
    input_file_content["class"] = "File"
    input_file_content.setdefault("conn_id", None)
    input_file_content.setdefault("is_dataframe", False)
    input_file_content.setdefault("normalize_config", None)

    operator_content["input_file"] = deserialize(input_file_content)

    output_table_content = operator_content.pop("output_table")
    output_table_content["class"] = "Table"
    output_table_content.setdefault("metadata", {})
    output_table_content.setdefault("name", operator_file_name)
    output_table_content.setdefault("temp", False)

    operator_content["output_table"] = deserialize(output_table_content)

    return operator_content


def get_load_file_instance(yaml_content: dict[str, Any], operator_file_name: str) -> LoadFileOperator:
    """
    Instantiate the LoadFileOperator.

    :param yaml_content: The content of the load_file yaml.
    :param operator_file_name: The file name of the operator.

    :returns: an instance of the LoadFileOperator
    """
    operator_content: dict[str, Any] = build_load_file_operator_content(yaml_content, operator_file_name)
    return LoadFileOperator(**operator_content)
