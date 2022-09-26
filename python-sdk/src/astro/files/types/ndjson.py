from __future__ import annotations

import io
import json
import time

import pandas as pd
from astro.constants import DEFAULT_CHUNK_SIZE
from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType
from astro.utils.dataframe import convert_columns_names_capitalization


class NDJSONFileType(FileType):
    """Concrete implementation to handle NDJSON file type"""

    def export_to_dataframe(
        self, stream, columns_names_capitalization="original", **kwargs
    ):
        """read ndjson file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        df = NDJSONFileType.flatten(self.normalize_config, stream, **kwargs)
        df = convert_columns_names_capitalization(
            df=df, columns_names_capitalization=columns_names_capitalization
        )
        return df

    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write ndjson file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_json(stream, orient="records", lines=True)

    @property
    def name(self):
        return FileTypeConstants.NDJSON

    @staticmethod
    def flatten(
        normalize_config: dict | None, stream: io.TextIOWrapper, **kwargs
    ) -> pd.DataFrame:
        """
        Flatten the nested ndjson/json.

        :param normalize_config: parameters in dict format of pandas json_normalize() function.
            https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html
        :param stream: io.TextIOWrapper object for the file
        :type normalize_config: dict
        :type stream: io.TextIOWrapper
        :return: return dataframe containing the loaded data
        :rtype: `pandas.DataFrame`
        """
        normalize_config = normalize_config or {}
        nrows = kwargs.get("nrows", float("inf"))
        chunksize = kwargs.get("chunksize", DEFAULT_CHUNK_SIZE)

        result_df = []
        row_count = 0
        extra_rows = []

        readlines_counter = 0.0
        flattening_counter = 0.0
        concat_counter = 0.0
        json_load_counter = 0.0

        while nrows and row_count < nrows:

            start_readlines = time.time()
            extra_rows.extend(stream.readlines(chunksize))
            readlines_counter = readlines_counter + (time.time() - start_readlines)

            rows = extra_rows
            if len(rows) == 0:
                break

            # Remove extra rows
            if nrows and nrows < row_count + len(rows):
                extra_rows = rows[nrows:]
                rows = rows[:nrows]
            else:
                extra_rows = []

            start_json_load = time.time()
            r = [json.loads(row) for row in rows]
            json_load_counter = json_load_counter + (time.time() - start_json_load)

            start_flattening = time.time()
            df = pd.json_normalize(r, **normalize_config)
            print(">>>>>>>>>>>>>>>>>>> \n", df)
            flattening_counter = flattening_counter + (time.time() - start_flattening)

            # if result_df is None:
            #     result_df = df
            # else:
            #
            #     start_concat = time.time()
            #     result_df = result_df.append(df)  # pd.concat([result_df, df])
            #     concat_counter = concat_counter + (time.time() - start_concat)
            result_df.append(df)
            row_count = row_count + df.shape[0]

        start_concat = time.time()
        finale_df = pd.concat(result_df)
        concat_counter = concat_counter + (time.time() - start_concat)

        print(">>>>>>>>>>>>>>> readlines_counter: ", readlines_counter)
        print(">>>>>>>>>>>>>>> json_load_counter: ", json_load_counter)
        print(">>>>>>>>>>>>>>> flattening_counter: ", flattening_counter)
        print(">>>>>>>>>>>>>>> concat_counter: ", concat_counter)

        return finale_df
