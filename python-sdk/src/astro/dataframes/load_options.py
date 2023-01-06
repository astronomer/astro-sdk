from __future__ import annotations

from attr import define
from pandas._typing import DtypeArg

from astro.options import LoadOptions


@define
class PandasLoadOptions(LoadOptions):
    pass


@define
class CsvLoadOptions(PandasLoadOptions):
    delimiter: str | None = None
    dtype: DtypeArg | None = None


@define
class JsonLoadOptions(PandasLoadOptions):
    encoding: str | None = None


@define
class NdjsonLoadOptions(PandasLoadOptions):
    ndjson_normalize_sep: str = "_"


@define
class ParquetLoadOptions(PandasLoadOptions):
    columns: list[str] | None = None
