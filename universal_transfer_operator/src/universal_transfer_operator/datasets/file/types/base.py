from __future__ import annotations

import io
from abc import ABC, abstractmethod

import pandas as pd


class FileType(ABC):
    """Abstract File type class, meant to be the interface to all client code for all supported file types"""

    def __init__(self, path: str, normalize_config: dict | None = None):
        self.path = path
        self.normalize_config = normalize_config

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

    def __str__(self):
        """String representation of type"""
        return self.name.value

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(path="{self.path}")'

    def __eq__(self, other):
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)
