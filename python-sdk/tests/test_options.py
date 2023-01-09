from astro.dataframes.load_options import (
    PandasCsvLoadOptions,
    PandasJsonLoadOptions,
    PandasNdjsonLoadOptions,
    PandasParquetLoadOptions,
)
from astro.options import LoadOptionsList


def test_load_options_list():
    """
    Test LoadOptionsList's get() method
    """
    load_option_list = LoadOptionsList(
        [
            PandasCsvLoadOptions(delimiter="$"),
            PandasJsonLoadOptions(encoding="test"),
            PandasParquetLoadOptions(columns=["name", "age"]),
            PandasNdjsonLoadOptions(ndjson_normalize_sep="__"),
        ]
    )
    csv_load_option = load_option_list.get_by_class_name("PandasCsvLoadOptions")
    assert csv_load_option.delimiter == "$"
    json_load_option = load_option_list.get_by_class_name("PandasJsonLoadOptions")
    assert json_load_option.encoding == "test"
    parquet_load_option = load_option_list.get_by_class_name("PandasParquetLoadOptions")
    assert parquet_load_option.columns == ["name", "age"]
    parquet_load_option = load_option_list.get_by_class_name("PandasNdjsonLoadOptions")
    assert parquet_load_option.ndjson_normalize_sep == "__"

    load_option = load_option_list.get_by_class_name("InvalidLoadOption")
    assert load_option is None
