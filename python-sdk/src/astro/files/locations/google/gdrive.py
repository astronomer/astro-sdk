from __future__ import annotations

import pathlib
import urllib.parse
from typing import Any, Iterator, Sequence

from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


def _quote(s: str) -> str:
    s = s.replace("'", "\\'")  # Escape single quote characters.
    return f"'{s}'"  # Quote.


def _find_item_id(path_parts: Sequence[str], *, conn: Any) -> str:
    """Drill down the directory hierarchy to find the ID of an item.

    Google Drive does not have a very good random access API by path, so we
    must look at each directory, level by level, to find the item we want.
    """
    folder_id = "root"
    for part in path_parts:
        command = conn.files().list(
            q=f"parents in {_quote(folder_id)} and name = {_quote(part)}",
            fields="id",
            pageSize="1",
        )
        response = command.execute()
        try:
            found = response["files"][0]
        except IndexError:
            raise FileNotFoundError
        folder_id = found["id"]
    return folder_id


def _find_contains(folder_id: str, pattern: str, *, conn: Any) -> Iterator[str]:
    """Find files in a directory matching given pattern.

    This uses *contains* to search. See Google Drive documentation to
    understand the implications:
    https://developers.google.com/drive/api/guides/ref-search-terms
    """
    page_token = None
    while True:
        command = conn.files().list(
            q=f"parents in {_quote(folder_id)} and name contains {_quote(pattern)}",
            fields="nextPageToken, files(name)",
            pageToken=page_token,
        )
        response = command.execute()
        yield from (f["name"] for f in response["files"])
        page_token = response["nextPageToken"]
        if page_token is None:
            return


class GdriveLocation(BaseFileLocation):
    """Handler for Google Drive operators."""

    location_type = FileLocation.GOOGLE_DRIVE

    @property
    def hook(self) -> GoogleDriveHook:
        if self.conn_id:
            return GoogleDriveHook(gcp_conn_id=self.conn_id)
        return GoogleDriveHook()

    @property
    def transport_params(self) -> dict:
        """Get Google Drive client."""
        return {"client": self.hook.get_conn()}

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return pathlib.PurePosixPath(urllib.parse.urlsplit(self.path).path).name

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return urllib.parse.urlsplit(self.path).path

    def _get_rel_path_parts(self) -> Sequence[str]:
        p = urllib.parse.urlsplit(self.path).path.lstrip("/")
        return pathlib.PurePosixPath(p).parts

    def _get_conn(self) -> Any:
        return self.hook.get_conn()

    @property
    def paths(self) -> list[str]:
        path_parts = self._get_rel_path_parts()
        if not path_parts:
            return []

        conn = self._get_conn()
        url = urllib.parse.urlsplit(self.path)
        parent_path = pathlib.PurePosixPath(*path_parts[:-1])
        try:
            folder_id = _find_item_id(path_parts[:-1], conn=conn)
        except FileNotFoundError:
            filename = urllib.parse.urlunsplit(url._replace(path=parent_path.as_posix()))
            raise FileNotFoundError(filename) from None
        return [
            urllib.parse.urlunsplit(url._replace(path=parent_path.joinpath(name).as_posix()))
            for name in _find_contains(folder_id, path_parts[-1], conn=conn)
        ]

    @property
    def size(self) -> int:
        conn = self._get_conn()
        try:
            file_id = _find_item_id(self._get_rel_path_parts(), conn=conn)
        except FileNotFoundError:
            raise FileNotFoundError(self.path) from None
        response = conn.files().get(fileId=file_id, fields="size").execute()
        try:
            value = response["size"]
        except KeyError:
            raise IsADirectoryError(self.path) from None
        return int(value)
