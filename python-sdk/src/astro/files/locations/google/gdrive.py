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


def _find_item_id(path_parts: str, *, conn: Any) -> str:
    """Drill down the directory hierarchy to find the ID of an item.

    Google Drive does not have a very good random access API by path, so we
    must look at each directory, level by level, to find the item we want.
    """
    current_parent = "root"
    folders = path_parts.split("/")
    # First tries to enter directories
    for current_folder in folders:
        conditions = [
            "mimeType = 'application/vnd.google-apps.folder'",
            f"name='{current_folder}'",
        ]
        if current_parent != "root":
            conditions.append(f"'{current_parent}' in parents")
        result = (
            conn.files().list(q=" and ".join(conditions), spaces="drive", fields="files(id, name)").execute()
        )
        files = result.get("files", [])
        if not files:
            # If the directory does not exist, break loops
            break
        current_parent = files[0].get("id")
    return current_parent


def _find_contains(folder_id: str, pattern: str, *, conn: Any) -> Iterator[str]:
    """Find files in a directory matching given pattern.

    This uses *contains* to search. See Google Drive documentation to
    understand the implications:
    https://developers.google.com/drive/api/guides/ref-search-terms
    """
    page_token = None
    while True:
        response = (
            conn.files()
            .list(
                q=f"name contains {_quote(pattern)} and {_quote(folder_id)} in parents ",
                fields="nextPageToken, files(name,id, mimeType,webContentLink,size)",
                pageToken=page_token,
            )
            .execute()
        )
        yield from (f["webContentLink"] for f in response["files"])
        page_token = response.get("nextPageToken", None)
        if page_token is None:
            return


class GdriveLocation(BaseFileLocation):
    """Handler for Google Drive operators."""

    location_type = FileLocation.GOOGLE_DRIVE
    supported_conn_type = {GoogleDriveHook.conn_type}

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
        parsed_url = urllib.parse.urlsplit(self.path)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

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
        result_paths = []
        try:
            folder_id = _find_item_id(url.netloc + url.path, conn=conn)
        except FileNotFoundError:
            raise FileNotFoundError(self.path) from None
        for name in _find_contains(folder_id, path_parts[-1], conn=conn):
            result_paths.append(name)
        return result_paths

    @property
    def size(self) -> int:
        path_parts = self._get_rel_path_parts()
        conn = self._get_conn()
        url = urllib.parse.urlsplit(self.path)
        try:
            folder_id = _find_item_id(url.netloc + url.path, conn=conn)
        except FileNotFoundError:
            raise FileNotFoundError(self.path) from None

        response = (
            conn.files()
            .list(
                q=f"name contains {_quote(path_parts[-1])} and {_quote(folder_id)} in parents ",
                fields="files(name,id,size)",
            )
            .execute()
        )
        value: int = 0
        try:
            for f in response["files"]:
                value = int(f["size"])
        except KeyError:
            raise IsADirectoryError(self.path) from None
        return value
