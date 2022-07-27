import pandas as pd

from astro.constants import ColumnCapitalization


def convert_columns_names_capitalization(
    df: pd.DataFrame, columns_names_capitalization: ColumnCapitalization
):
    """
    Convert cols of a dataframe to required case. Options - lower/Upper

    :param df: dataframe whose cols will be altered
    :param columns_names_capitalization: String Literal with possible values - lower/Upper
    """
    if isinstance(df, pd.DataFrame):
        if columns_names_capitalization == "lower":
            df.columns = [col_label.lower() for col_label in df.columns]
        elif columns_names_capitalization == "upper":
            df.columns = [col_label.upper() for col_label in df.columns]
    return df
