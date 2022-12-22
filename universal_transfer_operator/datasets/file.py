from __future__ import annotations

from attr import define, field

from astro.airflow.datasets import Dataset


@define
class File(Dataset):
    """
    Handle all file operations, and abstract away the details related to location and file types.
    Intended to be used within library.

    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    """

    path: str
    conn_id: str | None = None

    uri: str = field(init=False)
    extra: dict | None = field(init=False, factory=dict)

    template_fields = (
        "path",
        "conn_id",
    )
