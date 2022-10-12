from __future__ import annotations

import attr
from openlineage.client.facet import BaseFacet

from astro.constants import FileType
from astro.table import Column, Metadata


@attr.define
class InputFileDatasetFacet(BaseFacet):
    """
    Facet that represents input dataset Facets for load file

    :param file_size: size of the file.
    :param number_of_files: number of files to be loaded.
    :param file_type: type of the file.
    :param description: description of the dataset.
    :param is_pattern: True when file path is a pattern(eg. ``s3://bucket/folder`` or ``/folder/sample_*`` etc).
    :param files: list of filepaths to be loaded from dataset.
    """

    file_size: int | None
    number_of_files: int | None
    file_type: FileType
    description: str | None = None
    is_pattern: bool = False
    files: list[str] = []


@attr.define
class OutputDatabaseDatasetFacet(BaseFacet):
    """
    Facet that represents output dataset Facets for load file

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
    enabled_native_fallback: bool = False
    native_support_arguments: dict | None = None
    description: str | None = None

    _do_not_redact = ["metadata", "columns"]
