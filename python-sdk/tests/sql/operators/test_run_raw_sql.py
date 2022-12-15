import pathlib

import pytest

from astro import sql as aql

CWD = pathlib.Path(__file__).parent
DATA_FILEPATH = pathlib.Path(CWD.parent.parent, "data/sample.csv")


@pytest.mark.parametrize("rows", [([], []), (["a", "b"], ["a", "b"]), (1, 1), (None, None)])
def test_make_row_serializable(rows):
    """
    Test make_row_serializable() only modifies SQLAlcRow object
    """
    input_rows, expected_output = rows
    result = aql.RawSQLOperator.make_row_serializable(input_rows)
    assert result == expected_output
