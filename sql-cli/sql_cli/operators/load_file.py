from __future__ import annotations

import warnings
from typing import Any

from airflow import XComArg
from airflow.decorators.base import get_unique_task_id

from astro.constants import DEFAULT_CHUNK_SIZE, ColumnCapitalization, LoadExistStrategy
from astro.custom_backend.serializer import deserialize
from astro.files import File
from astro.options import LoadOptions
from astro.settings import LOAD_FILE_ENABLE_NATIVE_FALLBACK
from astro.sql import LoadFileOperator
from astro.sql.operators.load_file import check_if_connection_exists
from astro.table import BaseTable
from astro.utils.compat.typing import Context


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


class NominalLoadFileOperator(LoadFileOperator):
    """Load S3/local file into either a database or a pandas dataframe

    :param input_file: File path and conn_id for object stores
    :param output_table: Table to create
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
    :param chunk_size: Specify the number of records in each batch to be written at a time.
    :param if_exists: Overwrite file if exists. Default False.
    :param use_native_support: Use native support for data transfer if available on the destination.
    :param native_support_kwargs: kwargs to be used by method involved in native support flow
    :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
    :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer

    :return: If ``output_table`` is passed this operator returns a Table object. If not
        passed, returns a dataframe.
    """

    def __init__(
        self,
        input_file: File,
        output_table: BaseTable | None = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        if_exists: LoadExistStrategy = "replace",
        ndjson_normalize_sep: str = "_",
        use_native_support: bool = True,
        native_support_kwargs: dict | None = None,
        load_options: list[LoadOptions] | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
        enable_native_fallback: bool | None = LOAD_FILE_ENABLE_NATIVE_FALLBACK,
        **kwargs,
    ):
        super().__init__(
            input_file=input_file,
            output_table=output_table,
            chunk_size=chunk_size,
            if_exists=if_exists,
            ndjson_normalize_sep=ndjson_normalize_sep,
            use_native_support=use_native_support,
            native_support_kwargs=native_support_kwargs,
            load_options=load_options,
            columns_names_capitalization=columns_names_capitalization,
            enable_native_fallback=enable_native_fallback,
            **kwargs,
        )

    def execute(self, context: Context) -> BaseTable | File | None:  # skipcq: PYL-W0613
        """
        Load an existing dataset from a supported file into a SQL table or a Dataframe.
        """
        if self.input_file.conn_id:
            check_if_connection_exists(self.input_file.conn_id)

        # TODO: remove pushing to XCom once we update the airflow version.
        if self.output_table:
            context["ti"].xcom_push(key="output_table_conn_id", value=str(self.output_table.conn_id))
            context["ti"].xcom_push(key="output_table_name", value=str(self.output_table.name))
        # Skips the core execution of the operator but run all ancillary operations. This is useful for local run of
        # tasks where subsequent tasks in the DAG might need the output of this operator like XCOM results, but they
        # do not want to actually run the core logic and hence not cause effects outside the system.
        return self.output_table


def nominal_load_file(
    input_file: File,
    output_table: BaseTable | None = None,
    task_id: str | None = None,
    if_exists: LoadExistStrategy = "replace",
    ndjson_normalize_sep: str = "_",
    use_native_support: bool = True,
    native_support_kwargs: dict | None = None,
    columns_names_capitalization: ColumnCapitalization = "original",
    enable_native_fallback: bool | None = True,
    load_options: list[LoadOptions] | None = None,
    **kwargs: Any,
) -> XComArg:
    """Load a file or bucket into either a SQL table or a pandas dataframe.

    :param input_file: File path and conn_id for object stores
    :param output_table: Table to create
    :param task_id: task id, optional
    :param if_exists: default override an existing Table. Options: fail, replace, append
    :param ndjson_normalize_sep: separator used to normalize nested ndjson.
        ex - ``{"a": {"b":"c"}}`` will result in: ``column - "a_b"`` where ``ndjson_normalize_sep = "_"``
    :param use_native_support: Use native support for data transfer if available on the destination.
    :param native_support_kwargs: kwargs to be used by method involved in native support flow
    :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
        in the resulting dataframe
    :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer
    :param load_options: load options while reading and loading file
    """

    # Note - using path for task id is causing issues as it's a pattern and
    # contain chars like - ?, * etc. Which are not acceptable as task id.
    task_id = task_id if task_id is not None else get_unique_task_id("load_file")

    if native_support_kwargs:
        warnings.warn(
            """`load_options` will replace `native_support_kwargs` parameter in astro-sdk-python>=1.5.0. Please use
            `load_options` parameter instead.""",
            DeprecationWarning,
            stacklevel=2,
        )

    return NominalLoadFileOperator(
        task_id=task_id,
        input_file=input_file,
        output_table=output_table,
        if_exists=if_exists,
        ndjson_normalize_sep=ndjson_normalize_sep,
        use_native_support=use_native_support,
        native_support_kwargs=native_support_kwargs,
        columns_names_capitalization=columns_names_capitalization,
        enable_native_fallback=enable_native_fallback,
        load_options=load_options,
        **kwargs,
    ).output
