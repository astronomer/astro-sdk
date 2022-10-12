from typing import Dict, List, Optional

import attr
from openlineage.client.facet import BaseFacet

from astro.constants import FileType
from astro.table import Column, Metadata


@attr.s
class InputFileDatasetFacet(BaseFacet):
    """
    Facet that represents input dataset Facets for load file

    :param file_size: size of the file.
    :param number_of_files: number of files to be loaded.
    :param type: type of the file.
    :param description: description of the dataset.
    :param is_pattern: True when file path is a pattern(eg. s3://bucket/folder or /folder/sample_* etc).
    :param files: list of filepaths to be loaded from dataset.
    """

    file_size: Optional[int] = attr.ib()
    number_of_files: Optional[int] = attr.ib()
    file_type: FileType = attr.ib()
    description: Optional[str] = attr.ib(default=None)
    is_pattern: bool = attr.ib(default=False)
    files: List[str] = attr.ib(default=[])


@attr.s
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

    metadata: Metadata = attr.ib()
    columns: List[Column] = attr.ib()
    schema: Optional[str] = attr.ib()
    used_native_path: bool = attr.ib(default=False)
    enabled_native_fallback: Optional[bool] = attr.ib(default=False)
    native_support_arguments: Dict = attr.ib(default=None)
    description: Optional[str] = attr.ib(default=None)

    _do_not_redact = ["metadata", "columns"]
