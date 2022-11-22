from __future__ import annotations

from airflow.utils.log.logging_mixin import LoggingMixin
from attr import define, field

from astro.airflow.datasets import Dataset


@define
class UniversalDataset(LoggingMixin, Dataset):
    """
    Repersents all file dataset, and abstract away the details related to location and file types.
    Intended to be used within library.

    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    """

    path: str
    conn_id: str
    uri: str = field(init=False)
    extra: dict | None = field(init=False, factory=dict)

    template_fields = (
        "path",
        "conn_id",
    )
