from astro.utils.dataframe import convert_to_file, convert_file_to_dataframe
import pandas as pd


def test_convert_to_file():
    lst = {'col1': [1, 2], 'col2': [3, 4]}

    # Calling DataFrame constructor on list
    df = pd.DataFrame(lst)
    f = convert_to_file(df)
    out = convert_file_to_dataframe(f)
    print(out)