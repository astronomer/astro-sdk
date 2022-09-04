from __future__ import annotations

import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType


class PatternFileType(FileType):
    """Concrete implementation to handle file pattern type"""

    def export_to_dataframe(
        self, stream, columns_names_capitalization="original", **kwargs
    ):
        raise NotImplementedError

    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        raise NotImplementedError

    @property
    def name(self):
        return FileTypeConstants.PATTERN
