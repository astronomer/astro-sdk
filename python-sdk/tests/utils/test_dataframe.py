import pandas as pd

from astro.utils.dataframe import convert_to_file


def test_convert_to_file():
    """Ensures that there is parity when converting to file and exporting back to dataframe"""
    data_for_df = {"col1": [1, 2], "col2": [3, 4]}
    # Calling DataFrame constructor on dict
    df = pd.DataFrame(data_for_df)
    f = convert_to_file(df)
    out = f.export_to_dataframe()
    assert df.equals(out)
