from __future__ import annotations

from attr import define
from pandas._typing import DtypeArg

from astro.options import LoadOptions


@define
class PandasLoadOptions(LoadOptions):
    pass


@define
class PandasCsvLoadOptions(PandasLoadOptions):
    delimiter: str | None = None
    dtype: DtypeArg | None = None


@define
class PandasJsonLoadOptions(PandasLoadOptions):
    encoding: str | None = None


@define
class PandasNdjsonLoadOptions(PandasLoadOptions):
    ndjson_normalize_sep: str = "_"


@define
class PandasParquetLoadOptions(PandasLoadOptions):
    columns: list[str] | None = None
