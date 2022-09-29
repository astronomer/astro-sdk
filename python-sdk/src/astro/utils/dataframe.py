import random
import string

import pandas as pd
from astro.constants import ColumnCapitalization, FileType
from astro.settings import DATAFRAME_STORAGE_CONN_ID, DATAFRAME_STORAGE_URL


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


def convert_to_file(df: pd.DataFrame):
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(64)
    )
    from astro.files import File

    file = File(
        path=DATAFRAME_STORAGE_URL + "/" + unique_id + ".parquet",
        conn_id=DATAFRAME_STORAGE_CONN_ID,
        filetype=FileType.PARQUET,
        is_dataframe=True,
    )
    file.create_from_dataframe(df)
    return file


def convert_file_to_dataframe(f):
    from astro.files import File

    f: File = f
    return f.export_to_dataframe()
