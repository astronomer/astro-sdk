import pathlib

from astro import databases, settings
from astro.databases import sqlite
from astro.utils.path import (
    get_dict_with_module_names_to_dot_notations,
    get_module_dot_notation
)


def test_get_dict_with_module_names_to_dot_notations():
    databases_path = pathlib.Path(databases.__file__)
    paths = get_dict_with_module_names_to_dot_notations(databases_path)
    assert paths["sqlite"] == "astro.databases.sqlite"  # a module in the base folder
    assert (
        paths["bigquery"] == "astro.databases.google.bigquery"
    )  # a path in a subfolder


def test_get_module_dot_notation():
    settings_path = pathlib.Path(settings.__file__)
    path = get_module_dot_notation(settings_path)
    assert path == "astro.settings"

    google_databases_path = pathlib.Path(sqlite.__file__)
    path = get_module_dot_notation(google_databases_path)
    assert path == "astro.databases.sqlite"
