import importlib

from transfers.constants import IngestorSupported
from transfers.integrations.base import TransferIntegration

from astro.utils.path import get_class_name

CUSTOM_INGESTION_TYPE_TO_MODULE_PATH = {"Fivetran": "transfers.integrations.fivetran"}


def create_transfer_integration(transfer_params: dict = {}) -> TransferIntegration:
    """
    Given a transfer_params return the associated TransferIntegrations class.

    :param transfer_params: kwargs to be used by methods involved in transfer using FiveTran.
    """
    ingestion_type_name = transfer_params.get("ingestion_type", None)
    if ingestion_type_name is None:
        raise ValueError("ingestion_type not specified.")
    if ingestion_type_name not in {item.value for item in IngestorSupported}:
        raise ValueError("Ingestion platform not yet supported.")

    module_path = CUSTOM_INGESTION_TYPE_TO_MODULE_PATH[ingestion_type_name]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="Integration")
    transfer_integrations: TransferIntegration = getattr(module, class_name)(transfer_params)
    return transfer_integrations
