"""Tests specific to the Snowflake Database implementation."""
import pathlib

import pytest

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.database.snowflake import SnowflakeDataProvider
from universal_transfer_operator.datasets.table import Table

DEFAULT_CONN_ID = "snowflake_default"
CUSTOM_CONN_ID = "snowflake_conn"
SUPPORTED_CONN_IDS = [CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "cols_eval",
    [
        # {"cols": ["SELL", "LIST"], "expected_result": False},
        {"cols": ["Sell", "list"], "expected_result": True},
        {"cols": ["sell", "List"], "expected_result": True},
        {"cols": ["sell", "lIst"], "expected_result": True},
        {"cols": ["sEll", "list"], "expected_result": True},
        {"cols": ["sell", "LIST"], "expected_result": False},
        {"cols": ["sell", "list"], "expected_result": False},
    ],
)
def test_use_quotes(cols_eval):
    """
    Verify the quotes addition only in case where we are having mixed case col names
    """
    dp = SnowflakeDataProvider(dataset=Table(name="some_table"), transfer_mode=TransferMode.NONNATIVE)
    assert dp.use_quotes(cols_eval["cols"]) == cols_eval["expected_result"]
