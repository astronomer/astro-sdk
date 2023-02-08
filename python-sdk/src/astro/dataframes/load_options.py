from __future__ import annotations

import attr
from pandas._typing import DtypeArg

from astro.options import LoadOptions


@attr.define
class PandasLoadOptions(LoadOptions):
    """
    Pandas load options while reading and loading csv file.

    :param delimiter: valid file type[CSV] - Delimiter to use. Defaults to None
    :param dtype: valid file type[CSV] - Data type for data or columns.
        E.g. ``{"a": np.float64, "b": np.int32, "c": "Int64"}`` Use str or
        object together with suitable na_values settings to preserve and not interpret dtype. If converters are
        specified, they will be applied INSTEAD of dtype conversion.
    :param encoding: valid file type[JSON/NDJSON] - Encoding to use for UTF when reading/writing (ex. ‘utf-8’).
        List of Python standard encodings: https://docs.python.org/3/library/codecs.html#standard-encodings
    :param normalize_sep: valid file type[Parquet] - separator used to normalize nested ndjson.
         ex - ``{"a": {"b":"c"}}`` will result in: ``column - "a_b"`` where ``ndjson_normalize_sep = "_"``
    :param columns: If not None, only these columns will be read from the file.
    """

    # CSV
    delimiter: str | None = None
    dtype: DtypeArg | None = None

    # JSON
    encoding: str | None = None

    # NDJSON
    normalize_sep: str | None = None

    # Parquet
    columns: list[str] | None = None

    kwargs: dict = attr.field(init=True, factory=dict)

    def populate_kwargs(self, kwargs):
        exclude_key = ["kwargs"]
        kwargs.update(
            {
                key: val
                for key, val in self.load_options.to_dict().items()
                if val is not None and key not in exclude_key
            }
        )
        for key in exclude_key:
            kwargs.update(self.load_options.to_dict()[key])
        return kwargs
