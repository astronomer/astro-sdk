import pandas as pd
from astro.utils.dataframe import convert_to_file


def test_convert_to_file():
    lst = {"col1": [1, 2], "col2": [3, 4]}

    # Calling DataFrame constructor on list
    df = pd.DataFrame(lst)
    f = convert_to_file(df)
    out = f.export_to_dataframe()
    assert df.equals(out)
