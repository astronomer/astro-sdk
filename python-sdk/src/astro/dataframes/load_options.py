from __future__ import annotations

from attr import define
from pandas._typing import DtypeArg

from astro.options import LoadOptions


@define
class PandasLoadOptions(LoadOptions):
    """Pandas load options while reading and loading file"""

    pass


@define
class PandasCsvLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading csv file.

    :param delimiter: Delimiter to use. Defaults to None
    :param dtype: Data type for data or columns. E.g. ``{"a": np.float64, "b": np.int32, "c": "Int64"}`` Use str or
        object together with suitable na_values settings to preserve and not interpret dtype. If converters are
        specified, they will be applied INSTEAD of dtype conversion.
    """

    delimiter: str | None = None
    dtype: DtypeArg | None = None


@define
class PandasJsonLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading json file.

    :param encoding: Encoding to use for UTF when reading/writing (ex. ‘utf-8’).
        List of Python standard encodings: https://docs.python.org/3/library/codecs.html#standard-encodings
    """

    encoding: str | None = None


@define
class PandasNdjsonLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading Ndjson file.

    :param normalize_sep: separator used to normalize nested ndjson.
         ex - ``{"a": {"b":"c"}}`` will result in: ``column - "a_b"`` where ``ndjson_normalize_sep = "_"``
    """

    normalize_sep: str = "_"


@define
class PandasParquetLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading Parquet file.

    :param columns: If not None, only these columns will be read from the file.
    """

    columns: list[str] | None = None
