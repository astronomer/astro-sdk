from __future__ import annotations

from attr import define

from universal_transfer_operator.datasets.base import UniversalDataset


@define
class API(UniversalDataset):
    """
    Repersents all API dataset.
    Intended to be used within library.

    :param path: Path to a file in the filesystem/Object stores
    """

    # TODO: define the name and namespace for API
