import importlib
import os
from unittest.mock import patch

import astro
from astro import settings
from astro.files import File


def test_settings_load_table_schema_exists_default():
    from astro.sql import LoadFileOperator

    load_file = LoadFileOperator(input_file=File("dummy.csv"))
    assert not load_file.schema_exists


@patch.dict(os.environ, {"AIRFLOW__ASTRO_SDK__LOAD_TABLE_SCHEMA_EXISTS": "True"})
def test_settings_load_table_schema_exists_override():
    settings.reload()
    importlib.reload(astro.sql.operators.load_file)
    load_file = astro.sql.operators.load_file.LoadFileOperator(input_file=File("dummy.csv"))
    assert load_file.schema_exists
