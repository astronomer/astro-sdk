from __future__ import annotations

import logging
from functools import cached_property
from typing import Any

import attr
from attr import field
from fivetran_provider.hooks.fivetran import FivetranHook

from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.integrations.base import TransferIntegration, TransferIntegrationOptions


@attr.define
class Group:
    """
    Fivetran group details.

    :param name: The name of the group within Fivetran account.
    :param group_id: Group id in fivetran system

    """

    name: str
    group_id: str | None = None

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return attr.asdict(self)


@attr.define
class Destination:
    """
    Fivetran destination details

    :param service: services as per https://fivetran.com/docs/rest-api/destinations/config
    :param config: Configuration as per destination specified at https://fivetran.com/docs/rest-api/destinations/config
    :param destination_id: The unique identifier for the destination within the Fivetran system
    :param time_zone_offset: Determines the time zone for the Fivetran sync schedule.
    :param region: Data processing location. This is where Fivetran will operate and run computation on data.
    :param run_setup_tests: Specifies whether setup tests should be run automatically.
    """

    service: str
    config: dict
    destination_id: str | None = None
    time_zone_offset: str | None = "-5"
    region: str | None = "GCP_US_EAST4"
    run_setup_tests: bool | None = True

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return attr.asdict(self)


@attr.define
class Connector:
    """
    Fivetran connector details

    :param connector_id: The unique identifier for the connector within the Fivetran system
    :param service: services as per https://fivetran.com/docs/rest-api/destinations/config
    :param config: Configuration as per destination specified at https://fivetran.com/docs/rest-api/connectors/config
    :param paused: Specifies whether the connector is paused. Defaults to True.
    :param pause_after_trial: Specifies whether the connector should be paused after the free trial period has ended.
     Defaults to True.
    :param sync_frequency: The connector sync frequency in minutes Enum: "5" "15" "30" "60" "120" "180" "360" "480"
     "720" "1440". Default to "5"
    :param daily_sync_time: Defines the sync start time when the sync frequency is already set or being set by
     the current request to 1440.
    :param schedule_type: Define the schedule type
    :param connect_card_config: Connector card configuration
    :param trust_certificates: Specifies whether we should trust the certificate automatically. The default value is
     FALSE. If a certificate is not trusted automatically,
    :param trust_fingerprints: Specifies whether we should trust the SSH fingerprint automatically. The default value
     is FALSE.
    :param run_setup_tests: Specifies whether the setup tests should be run automatically. The default value is TRUE.
    """

    connector_id: str | None
    service: str
    config: dict
    connect_card_config: dict
    paused: bool = True
    pause_after_trial: bool = True
    sync_frequency: str = "5"
    daily_sync_time: str = "00:00"
    schedule_type: str = ""
    trust_certificates: bool = False
    trust_fingerprints: bool = False
    run_setup_tests: bool = True

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return attr.asdict(self)


@attr.define
class FiveTranOptions(TransferIntegrationOptions):
    """
    FiveTran load options.

    :param conn_id: Connection id of Fivetran
    :param connector_id: The unique identifier for the connector within the Fivetran system
    :param retry_limit: Retry limit. Defaults to 3
    :param retry_delay: Retry delay
    :param poll_interval: Polling interval. Defaults to 15 seconds
    :param schedule_type: Define the schedule type
    :param connector: Connector in FiveTran
    :param group: Group in FiveTran
    :param destination: Destination in Fivetran
    """

    conn_id: str = field(default="fivetran_default")
    connector_id: str | None = field(default="")
    retry_limit: int = 3
    retry_delay: int = 1
    poll_interval: int = 15
    schedule_type: str = "manual"
    connector: Connector | None = attr.field(default=None)
    group: Group | None = attr.field(default=None)
    destination: Destination | None = attr.field(default=None)

    def to_dict(self) -> dict:
        """
        Convert options class to dict
        """
        return attr.asdict(self)


class FivetranIntegration(TransferIntegration):
    """Fivetran integration to transfer datasets using Fivetran APIs."""

    api_user_agent = "airflow_provider_fivetran/1.1.3"
    api_protocol = "https"
    api_host = "api.fivetran.com"
    api_path_connectors = "v1/connectors/"
    api_path_groups = "v1/groups/"
    api_path_destinations = "v1/destinations/"

    def __init__(
        self,
        transfer_params: FiveTranOptions = attr.field(
            factory=FiveTranOptions,
            converter=lambda val: FiveTranOptions(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.transfer_params: FiveTranOptions = transfer_params
        self.transfer_mapping = {}
        super().__init__(transfer_params=self.transfer_params)

    @cached_property
    def hook(self) -> FivetranHook:
        """Return an instance of the database-specific Airflow hook."""
        return FivetranHook(
            self.transfer_params.conn_id,
            retry_limit=self.transfer_params.retry_limit,
            retry_delay=self.transfer_params.retry_delay,
        )

    def transfer_job(self, source_dataset: Dataset, destination_dataset: Dataset) -> Any:
        """
        Loads data from source dataset to the destination using ingestion config

        :param source_dataset: Source dataset
        :param destination_dataset: Destination dataset
        """
        if not source_dataset:
            raise ValueError("Source dataset is not specified.")

        if not destination_dataset:
            raise ValueError("Destination dataset is not specified.")

        fivetran_hook = self.hook

        # Check if connector_id is passed and check if it exists and do the transfer.
        connector_id = self.transfer_params.connector_id
        if self.check_for_connector_id(fivetran_hook=fivetran_hook):
            fivetran_hook.prep_connector(
                connector_id=connector_id, schedule_type=self.transfer_params.schedule_type
            )
            # TODO: wait until the job is done
            return fivetran_hook.start_fivetran_sync(connector_id=connector_id)

        group_id = (
            self.transfer_params.group.group_id
            if self.transfer_params.group and self.transfer_params.group.group_id
            else None
        )
        if not group_id or not self.check_group_details(fivetran_hook=fivetran_hook, group_id=group_id):
            # create group if not group_id is not passed.
            group_id = self.create_group(fivetran_hook=fivetran_hook)

        destination_id = (
            self.transfer_params.destination.destination_id
            if self.transfer_params.destination and self.transfer_params.destination.destination_id
            else None
        )
        if not destination_id or not self.check_destination_details(
            fivetran_hook=fivetran_hook, destination_id=destination_id
        ):
            # Check for destination based on destination_id else create destination
            self.create_destination(fivetran_hook=fivetran_hook, group_id=group_id)

        # Create connector if it doesn't exist
        connector_id = self.create_connector(fivetran_hook=fivetran_hook, group_id=group_id)

        # Run connector setup test
        self.run_connector_setup_tests(fivetran_hook=fivetran_hook, connector_id=connector_id)

        # Sync connector data
        fivetran_hook.prep_connector(
            connector_id=connector_id, schedule_type=self.transfer_params.schedule_type
        )
        return fivetran_hook.start_fivetran_sync(connector_id=connector_id)

    def check_for_connector_id(self, fivetran_hook: FivetranHook) -> bool:
        """
        Ensures connector configuration has been completed successfully and is in a functional state.

        :param fivetran_hook: Fivetran hook
        """
        connector_id = self.transfer_params.connector_id
        if connector_id is None:
            logging.warning("No value specified for connector_id")
            return False
        # Explicitly casting is required since the `fivetran_hook.check_connector` doesn't specify the return type
        return bool(fivetran_hook.check_connector(connector_id=connector_id))

    def check_group_details(self, fivetran_hook: FivetranHook, group_id: str | None) -> bool:
        """
        Check if group_id is exists.

        :param fivetran_hook: Fivetran hook
        :param group_id: Group id in fivetran system
        """

        if group_id is None:
            logging.warning(
                "group_id is None. It should be the unique identifier for "
                "the group within the Fivetran system. "
            )
            return False
        endpoint = self.api_path_groups + group_id
        api_response = fivetran_hook._do_api_call(("GET", endpoint))  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info("group_id {group_id} found.", extra={"group_id": group_id})
        else:
            raise ValueError(api_response)
        return True

    def create_group(self, fivetran_hook: FivetranHook) -> str:
        """
        Creates the group based on group name passed

        :param fivetran_hook: Fivetran hook
        """
        endpoint = self.api_path_groups
        group_dict = self.transfer_params.group
        if group_dict is None:
            raise ValueError("Group is none. Pass a valid group")
        group = Group(**group_dict.to_dict())
        payload = {"name": group.name}
        api_response = fivetran_hook._do_api_call(("POST", endpoint), json=payload)  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info(api_response)
        else:
            raise ValueError(api_response)
        return str(api_response["data"]["id"])

    def check_destination_details(self, fivetran_hook: FivetranHook, destination_id: str | None) -> bool:
        """
        Check if destination_id is exists.

        :param fivetran_hook: Fivetran hook
        :param destination_id: The unique identifier for the destination within the Fivetran system
        """
        if destination_id is None:
            logging.warning(
                "destination_id is None. It should be the unique identifier for "
                "the destination within the Fivetran system. "
            )
            return False
        endpoint = self.api_path_destinations + destination_id
        api_response = fivetran_hook._do_api_call(("GET", endpoint))  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info("destination_id {destination_id} found.", extra={"destination_id": destination_id})
        else:
            raise ValueError(api_response)
        return True

    def create_destination(self, fivetran_hook: FivetranHook, group_id: str) -> dict:
        """
        Creates the destination based on destination configuration passed

        :param fivetran_hook: Fivetran hook
        :param group_id: Group id in fivetran system
        """
        endpoint = self.api_path_destinations
        destination_dict = self.transfer_params.destination
        if destination_dict is None:
            raise ValueError("destination is none. Pass a valid destination")
        destination = Destination(**destination_dict.to_dict())
        payload = {
            "group_id": group_id,
            "service": destination.service,
            "region": destination.region,
            "time_zone_offset": destination.time_zone_offset,
            "config": destination.config,
            "run_setup_tests": destination.run_setup_tests,
        }
        api_response = fivetran_hook._do_api_call(("POST", endpoint), json=payload)  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info(api_response)
            # TODO: parse all setup tests status for passed status
        else:
            raise ValueError(api_response)
        return dict(api_response)

    def create_connector(self, fivetran_hook: FivetranHook, group_id: str) -> str:
        """
        Creates the connector based on connector configuration passed

        :param fivetran_hook: Fivetran hook
        :param group_id: Group id in fivetran system
        """
        endpoint = self.api_path_connectors
        connector_dict = self.transfer_params.connector
        if connector_dict is None:
            raise ValueError("connector is none. Pass a valid connector")

        connector = Connector(**connector_dict.to_dict())
        payload = {
            "group_id": group_id,
            "service": connector.service,
            "trust_certificates": connector.trust_certificates,
            "trust_fingerprints": connector.trust_fingerprints,
            "run_setup_tests": connector.run_setup_tests,
            "paused": connector.paused,
            "pause_after_trial": connector.pause_after_trial,
            "sync_frequency": connector.sync_frequency,
            "daily_sync_time": connector.daily_sync_time,
            "schedule_type": connector.schedule_type,
            "connect_card_config": connector.connect_card_config,
            "config": connector.config,
        }
        api_response = fivetran_hook._do_api_call(("POST", endpoint), json=payload)  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info(api_response)
            # TODO: parse all setup tests status for passed status
        else:
            raise ValueError(api_response)
        return str(api_response["data"]["id"])

    def run_connector_setup_tests(self, fivetran_hook: FivetranHook, connector_id: str):
        """
        Runs the setup tests for an existing connector within your Fivetran account.

        :param fivetran_hook: Fivetran hook
        :param connector_id: The unique identifier for the connector within the Fivetran system
        """
        endpoint = self.api_path_connectors + connector_id + "/test"
        connector_dict = self.transfer_params.connector
        if connector_dict is None:
            raise ValueError("connector is none. Pass a valid connector")

        connector = Connector(**connector_dict.to_dict())
        payload = {
            "trust_certificates": connector.trust_certificates,
            "trust_fingerprints": connector.trust_fingerprints,
        }
        api_response = fivetran_hook._do_api_call(("POST", endpoint), json=payload)  # skipcq: PYL-W0212
        if api_response["code"] == "Success":
            logging.info(api_response)
            # TODO: parse all setup tests status for passed status
        else:
            raise ValueError(api_response)
