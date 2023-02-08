from astro.dataframes.load_options import PandasLoadOptions
from astro.options import (
    LoadOptionsList,
    SnowflakeLoadOptions,
    WASBLocationLoadOptions,
    contains_required_option,
)


def test_load_options_list():
    """
    Test LoadOptionsList's get() method
    """
    load_option_list = LoadOptionsList(
        [
            PandasLoadOptions(delimiter="$", encoding="test", columns=["name", "age"], normalize_sep="__"),
            WASBLocationLoadOptions(storage_account="some_account"),
            SnowflakeLoadOptions(copy_options={"some_key": "some_val"}),
        ]
    )
    load_option = load_option_list.get_by_class_name("PandasLoadOptions")
    assert load_option.delimiter == "$"
    assert load_option.encoding == "test"
    assert load_option.columns == ["name", "age"]
    assert load_option.normalize_sep == "__"

    load_option = load_option_list.get_by_class_name("WASBLocationLoadOptions")
    assert load_option.storage_account == "some_account"

    load_option = load_option_list.get_by_class_name("SnowflakeLoadOptions")
    assert load_option.copy_options == {"some_key": "some_val"}

    load_option = load_option_list.get_by_class_name("InvalidLoadOption")
    assert load_option is None


def test_contains_required_option():
    """
    Test if a required param is present in a load_options class object
    """
    load_option = PandasLoadOptions(delimiter="$")
    assert contains_required_option(load_option, "delimiter") is True
    assert contains_required_option(load_option, "non-existing-option") is False
    assert contains_required_option(None, "non-existing-option") is False
