from __future__ import annotations

from airflow.utils.log.logging_mixin import LoggingMixin
from attr import define, field
from transfers.datasets.base import UniversalDataset


@define
class Table(LoggingMixin, UniversalDataset):
    """
    Repersents all Databases datasets, and abstract away the details related to database types.
    Intended to be used within library.

    :param path: Path to a database
    :param conn_id: Airflow connection ID
    """

    path: str
    conn_id: str

    uri: str = field(init=False)
    extra: dict = field(init=True, factory=dict)

    template_fields = ("path", "conn_id", "extra")

    @property
    def sql_type(self):
        raise NotImplementedError

    def exists(self) -> bool:
        """Check if the database exists or not"""
        database_exists: bool = self.data_provider.check_if_exists(self)
        return database_exists

    def __str__(self) -> str:
        return self.path

    def __hash__(self) -> int:
        return hash((self.path, self.conn_id))

    @uri.default
    def _path_to_dataset_uri(self) -> str:
        """Build a URI to be passed to Dataset obj introduced in Airflow 2.4"""
        from urllib.parse import urlencode, urlparse

        parsed_url = urlparse(url=self.path)
        netloc = parsed_url.netloc
        # Local filepaths do not have scheme
        parsed_scheme = parsed_url.scheme or "file"
        scheme = f"astro+{parsed_scheme}"
        extra = {}
        if self.filetype:
            extra["filetype"] = str(self.filetype)

        new_parsed_url = parsed_url._replace(
            netloc=f"{self.conn_id}@{netloc}" if self.conn_id else netloc,
            scheme=scheme,
            query=urlencode(extra),
        )
        return new_parsed_url.geturl()
