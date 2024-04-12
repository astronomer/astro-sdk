from __future__ import annotations

import attr
from openlineage.client.facet import BaseFacet

from astro.constants import ExportExistsStrategy, FileType, MergeConflictStrategy
from astro.table import Column, Metadata


@attr.define
class InputFileFacet(BaseFacet):
    """
    Facet that represents input file for load file

    :param filepath: path of each file.
    :param file_size: size of the file.
    :param file_type: type of the file.
    """

    filepath: str
    file_size: int | None
    file_type: FileType
    description: str | None = None


@attr.define
class ExportFileFacet(BaseFacet):
    """
    Facet that represents output file for export file

    :param filepath: path of each file.
    :param file_size: size of the file.
    :param file_type: type of the file.
    :param if_exists: Overwrite file if exists.
    """

    filepath: str
    file_size: int | None
    file_type: FileType
    if_exists: ExportExistsStrategy


@attr.define
class InputFileDatasetFacet(BaseFacet):
    """
    Facet that represents input file dataset Facets for load file

    :param file_size: size of the file.
    :param number_of_files: number of files to be loaded.
    :param file_type: type of the file.
    :param description: description of the dataset.
    :param is_pattern: True when file path is a pattern(eg. ``s3://bucket/folder`` or ``/folder/sample_*`` etc).
    :param files: list of filepaths to be loaded from dataset.
    """

    number_of_files: int | None
    description: str | None = None
    is_pattern: bool = False
    files: list[InputFileFacet] = []


@attr.define
class OutputDatabaseDatasetFacet(BaseFacet):
    """
    Facet that represents output database dataset Facets for load file

    :param metadata: metadata of the table.
    :param columns: columns defined in table
    :param schema: schema used.
    :param used_native_path: use native support for data transfer if available on the destination.
    :param enabled_native_fallback: use enable_native_fallback=True to fall back to default transfer.
    :param native_support_arguments: args to be used by method involved in native support flow
    :param description: description of the dataset.
    """

    metadata: Metadata
    columns: list[Column]
    schema: str | None
    used_native_path: bool
    enabled_native_fallback: bool | None = False
    native_support_arguments: dict | None = None
    description: str | None = None

    _do_not_redact = ["metadata", "columns"]


@attr.define
class TableDatasetFacet(BaseFacet):
    """
    Facets that represent may be available for Table object

    :param table_name: Name of the table
    :param columns: columns defined in table
    :param source_table_rows: Total rows in source table
    :param metadata: metadata of the table.
    """

    table_name: str
    columns: list[str] | tuple[str] | dict[str, str] | None = None
    source_table_rows: int = 0
    metadata: Metadata | None = None

    _do_not_redact = ["metadata", "columns"]


@attr.define
class SourceTableMergeDatasetFacet(BaseFacet):
    """
    Facets that represent may be available for Source Table object in MergeOperator

    :param table_name: Name of the table
    :param if_conflicts: The strategy to be applied if there are conflicts.
    :param columns: columns defined in table
    :param source_table_rows: Total rows in source table
    :param metadata: metadata of the table.
    """

    table_name: str
    if_conflicts: MergeConflictStrategy
    columns: list[str] | tuple[str] | dict[str, str] | None = None
    source_table_rows: int = 0
    metadata: Metadata | None = None

    _do_not_redact = ["metadata", "columns"]


@attr.define
class TargetTableMergeDatasetFacet(BaseFacet):
    """
    Facets that represent may be available for Target Table object in Merge Operator

    :param table_name: Name of the table
    :param target_conflict_columns: List of cols where we expect to have a conflict while combining
    :param columns: columns defined in table
    :param metadata: metadata of the table.
    """

    table_name: str
    target_conflict_columns: list[str]
    columns: list[str] | tuple[str] | dict[str, str] | None = None
    metadata: Metadata | None = None

    _do_not_redact = ["metadata", "columns"]
