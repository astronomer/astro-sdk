from __future__ import annotations

from attr import asdict, define
from pandas._typing import DtypeArg

from astro.options import LoadOptions


@define
class PandasLoadOptions(LoadOptions):
    pass


@define
class CsvLoadOption(PandasLoadOptions):
    delimiter: str | None = None
    dtype: DtypeArg | None = None


@define
class JsonLoadOption(PandasLoadOptions):
    encoding: str | None = None


@define
class NdjsonLoadOption(PandasLoadOptions):
    ndjson_normalize_sep: str = "_"


@define
class ParquetLoadOption(PandasLoadOptions):
    columns: list[str] | None = None
