from collections import deque

import numpy as np
import pandas as pd
import pytest

from astro.custom_backend.serializer import serialize
from astro.files import File
from astro.table import Table


@pytest.mark.parametrize(
    "req_param,expected",
    [
        (
            Table(name="astro", conn_id="astro_sdk"),
            {
                "class": "Table",
                "conn_id": "astro_sdk",
                "metadata": {"database": None, "schema": None},
                "name": "astro",
                "temp": False,
            },
        ),
        (
            File(path="astro", conn_id="astro_sdk"),
            {
                "class": "File",
                "conn_id": "astro_sdk",
                "filetype": None,
                "is_dataframe": False,
                "normalize_config": None,
                "path": "astro",
            },
        ),
        ([1, 2, "astro"], ["1", "2", {"class": "string", "value": "astro"}]),
        ({"software": "airflow"}, {"software": {"class": "string", "value": "airflow"}}),
        (pd.DataFrame(data={"col": [1, 2]}), {}),
        (int(2022), "2022"),
        (float(3.14), "3.14"),
        (np.int_(2022), 2022),
        (np.float_(3.14), 3.14),
        (np.int_([3, 1, 4]), [3, 1, 4]),
        ("astro", {"class": "string", "value": "astro"}),
        (deque(), deque([])),
    ],
)
def test_serialize(req_param, expected):
    actual = serialize(req_param)
    if isinstance(req_param, pd.DataFrame):
        assert actual["path"] is not None
        assert actual["is_dataframe"] is True
    else:
        assert actual == expected
