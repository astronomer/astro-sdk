from __future__ import annotations

from attr import define

from universal_transfer_operator.datasets.base import UniversalDataset


@define
class Dataframe(UniversalDataset):
    """
    Repersents all dataframe dataset.
    Intended to be used within library.

    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    :param name: name of dataframe
    """

    name: str | None = None

    # TODO: define the name and namespace for dataframe
