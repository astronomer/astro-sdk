import io
from abc import ABC, abstractmethod

import pandas as pd


class FileType(ABC):
    def __init__(self, path: str):
        self.path = path

    @abstractmethod
    def export_to_dataframe(self, stream, **kwargs) -> pd.DataFrame:
        """read file from one of the supported locations and return dataframe

        :param stream: file stream object
        """
        raise NotImplementedError

    @abstractmethod
    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def name(self):
        """get file type"""
        raise NotImplementedError
