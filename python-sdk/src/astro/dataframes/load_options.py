from __future__ import annotations

from attr import asdict, define

from astro.options import LoadOptions


@define
class PandasLoadOptions(LoadOptions):
    @property
    def to_dict(self):
        return dict(filter(lambda x: x[1], asdict(self).items()))


@define
class CsvLoadOption(PandasLoadOptions):
    delimiter: str | None = None


@define
class JsonLoadOption(PandasLoadOptions):
    encoding: str | None = None


@define
class NdjsonLoadOption(PandasLoadOptions):
    ndjson_normalize_sep: str = "_"


@define
class ParquetLoadOption(PandasLoadOptions):
    columns: list[str] | None = None
