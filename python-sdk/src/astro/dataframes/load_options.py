from __future__ import annotations

import warnings

import attr
from pandas._typing import DtypeArg

from astro.options import LoadOptions


@attr.define
class PandasLoadOptions(LoadOptions):
    """
    Pandas load options while reading and loading different files. Some common used params are added to the class for
     other valid options ref below. They can be passed in kwargs param:
     1. CSV file type - https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
     2. NDJSON/JSON file type - https://pandas.pydata.org/docs/reference/api/pandas.read_json.html
     3. Parquet file type - https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html

    :param delimiter: Delimiter to use. Defaults to None
    :param dtype: Data type for data or columns.
        E.g. ``{"a": np.float64, "b": np.int32, "c": "Int64"}`` Use str or
        object together with suitable na_values settings to preserve and not interpret dtype. If converters are
        specified, they will be applied INSTEAD of dtype conversion.
    :param encoding: Encoding to use for UTF when reading/writing (ex. ‘utf-8’).
        List of Python standard encodings: https://docs.python.org/3/library/codecs.html#standard-encodings
    :param normalize_sep: separator used to normalize nested ndjson.
         ex - ``{"a": {"b":"c"}}`` will result in: ``column - "a_b"`` where ``ndjson_normalize_sep = "_"``
    :param columns: If not None, only these columns will be read from the file.
    """

    delimiter: str | None = None
    normalize_sep: str | None = None
    columns: list[str] | None = None
    dtype: DtypeArg | None = None
    encoding: str | None = None
    kwargs: dict = attr.field(init=True, factory=dict)

    def populate_kwargs(self, kwargs):
        exclude_key = ["kwargs"]
        kwargs.update(
            {key: val for key, val in self.to_dict().items() if val is not None and key not in exclude_key}
        )
        for key in exclude_key:
            kwargs.update(self.to_dict()[key])
        return kwargs


class PandasCsvLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading csv file.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated, and will be removed in astro-sdk-python>=2.0.0.
            Please use `astro.dataframe.load_options.PandasLoadOptions`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class PandasJsonLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading json file.

    :param encoding: Encoding to use for UTF when reading/writing (ex. ‘utf-8’).
        List of Python standard encodings: https://docs.python.org/3/library/codecs.html#standard-encodings
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated, and will be removed in astro-sdk-python>=2.0.0.
            Please use `astro.dataframe.load_options.PandasLoadOptions`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class PandasNdjsonLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading Ndjson file.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated, and will be removed in astro-sdk-python>=2.0.0.
            Please use `astro.dataframe.load_options.PandasLoadOptions`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)


class PandasParquetLoadOptions(PandasLoadOptions):
    """
    Pandas load options while reading and loading Parquet file.
    """

    def __init__(self, *args, **kwargs):
        warnings.warn(
            """This class is deprecated, and will be removed in astro-sdk-python>=2.0.0.
            Please use `astro.dataframe.load_options.PandasLoadOptions`.""",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(*args, **kwargs)
