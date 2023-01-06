from astro.dataframes.load_options import CsvLoadOption, JsonLoadOption, NdjsonLoadOption, ParquetLoadOption
from astro.options import LoadOptionsList


def test_load_options_list():
    """
    Test LoadOptionsList's get() method
    """
    load_option_list = LoadOptionsList(
        [
            CsvLoadOption(delimiter="$"),
            JsonLoadOption(encoding="test"),
            ParquetLoadOption(columns=["name", "age"]),
            NdjsonLoadOption(ndjson_normalize_sep="__"),
        ]
    )
    csv_load_option = load_option_list.get_by_class_name("CsvLoadOption")
    assert csv_load_option.delimiter == "$"
    json_load_option = load_option_list.get_by_class_name("JsonLoadOption")
    assert json_load_option.encoding == "test"
    parquet_load_option = load_option_list.get_by_class_name("ParquetLoadOption")
    assert parquet_load_option.columns == ["name", "age"]
    parquet_load_option = load_option_list.get_by_class_name("NdjsonLoadOption")
    assert parquet_load_option.ndjson_normalize_sep == "__"

    load_option = load_option_list.get_by_class_name("InvalidLoadOption")
    assert load_option is None
