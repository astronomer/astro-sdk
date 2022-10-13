from __future__ import annotations

import random
import string
from typing import TYPE_CHECKING

import pandas as pd

from astro.constants import ColumnCapitalization, FileType
from astro.settings import DATAFRAME_STORAGE_CONN_ID, DATAFRAME_STORAGE_URL

if TYPE_CHECKING:
    from astro.files import File


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


def convert_dataframe_to_file(df: pd.DataFrame) -> File:
    """
    Passes a dataframe into a File using parquet as an efficient storage format. This allows us to use
    Json as a storage method without filling the metadata database. the values for conn_id and bucket path can
    be found in the airflow.cfg as follows:

    [astro]
    dataframe_storage_conn_id=...
    dataframe_storage_url=///
    :param df: Dataframe to convert to file
    :return: File object with reference to stored dataframe file
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(64)
    )

    # importing here to prevent circular imports
    from astro.files import File

    file = File(
        path=DATAFRAME_STORAGE_URL + "/" + unique_id + ".parquet",
        conn_id=DATAFRAME_STORAGE_CONN_ID,
        filetype=FileType.PARQUET,
        is_dataframe=True,
    )
    file.create_from_dataframe(df)
    return file
