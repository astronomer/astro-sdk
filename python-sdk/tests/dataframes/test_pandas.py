from contextlib import nullcontext as does_not_raise
from unittest import mock

import pandas as pd
import pytest

from astro import settings
from astro.dataframes.pandas import PandasDataframe
from astro.exceptions import AstroSDKConfigError


def test_from_pandas_df():
    df = pd.DataFrame([{"id": 1, "name": "xyz"}, {"id": 2, "name": "abc"}])
    astro_df = PandasDataframe.from_pandas_df(df)
    assert isinstance(astro_df, (PandasDataframe, pd.DataFrame))
    assert df.equals(astro_df)


@mock.patch("astro.utils.dataframe.settings.NEED_CUSTOM_SERIALIZATION", False)
def test_from_pandas_df_returns_pandas_type():
    """
    Test that PandasDataframe is unchanged when a Custom XCom backend is used or XCom pickling is enabled
    or Airflow version is <2.5
    """
    df = pd.DataFrame([{"id": 1, "name": "xyz"}, {"id": 2, "name": "abc"}])
    astro_df = PandasDataframe.from_pandas_df(df)
    assert not isinstance(astro_df, PandasDataframe)
    assert isinstance(astro_df, pd.DataFrame)
    assert df.equals(astro_df)


@pytest.mark.parametrize(
    "expectation,conn_id",
    [
        [does_not_raise(), "df_storage_conn_id"],
        [pytest.raises(AstroSDKConfigError), None],
    ],
)
def test_serialize_deserialize_with_larger_df(tmp_path, expectation, conn_id):
    """
    Test that we do not store the entire dataframe in DB if it is greater and we can correctly
    serialize and deserialize it
    """
    temp_dir = tmp_path / "test_dir"
    temp_dir.mkdir()
    assert [f for f in temp_dir.iterdir() if f.is_file()] == []

    with expectation:
        # Set the max size to allow storing Dataframe in DB to be 50kb
        with mock.patch(
            "astro.dataframes.pandas.settings.MAX_DATAFRAME_MEMORY_FOR_XCOM_DB", new=50
        ), mock.patch("astro.utils.dataframe.settings.DATAFRAME_STORAGE_URL", new=str(temp_dir)), mock.patch(
            "astro.utils.dataframe.settings.DATAFRAME_STORAGE_CONN_ID", new=conn_id
        ):
            # Create dataframe that is greater than 50kb
            records = [{"id": i, "name": "xyz"} for i in range(1000)]
            df = PandasDataframe(records)
            # Assert that size of DF> 50kb
            assert df.memory_usage(deep=True).sum() > (50 * 1024)
            # Test that the serialize method will not serialize all the dataframe records to string
            # and instead create a file object and store the records in a file
            s_df = df.serialize()
            assert s_df == {
                "class": "File",
                "conn_id": conn_id,
                "path": mock.ANY,
                "filetype": "parquet",
                "normalize_config": None,
                "is_dataframe": True,
            }

            # Test that a parquet file is created
            file_in_dir = [f for f in temp_dir.iterdir() if f.is_file() and f.name.endswith(".parquet")]
            assert file_in_dir

            # Test that we are able to get a dataframe back
            assert df.equals(PandasDataframe.deserialize(s_df, version=1))


def test_serialize_deserialize_with_smaller_df():
    """
    Test that we store the entire dataframe in DB if it is smaller and we can correctly
    serialize and deserialize it
    """

    # Create dataframe that is smaller
    records = [{"id": 1, "name": "xyz"}]
    df = PandasDataframe(records)

    # Assert that size of DF < MAX_DATAFRAME_MEMORY_FOR_XCOM_DB
    assert df.memory_usage(deep=True).sum() < (settings.MAX_DATAFRAME_MEMORY_FOR_XCOM_DB * 1024)

    # Test that the serialize method will not serialize all the dataframe records to string
    # and instead create a file object and store the records in a file
    s_df = df.serialize()
    assert s_df == {"data": '{"id":{"0":1},"name":{"0":"xyz"}}'}

    assert df.equals(PandasDataframe.deserialize(s_df, version=1))
