from __future__ import annotations

from airflow.configuration import conf
from attr import define, field

from astro.constants import LoadExistStrategy
from astro.options import LoadOptions


@define
class DeltaLoadOptions(LoadOptions):
    """Helper class for rendering options into COPY_INTO Databricks SQL statements"""

    copy_into_format_options: dict = field(init=True, factory=dict)
    copy_into_copy_options: dict = field(init=True, factory=dict)
    existing_cluster_id: str = field()
    new_cluster_spec: dict = field(init=True, factory=dict)
    if_exists: LoadExistStrategy = "replace"
    secret_scope: str = "astro-sdk-secrets"
    load_secrets: bool = False

    @existing_cluster_id.default
    def get_existing_cluster_id(self):
        return conf.get("astro_sdk", "databricks_cluster_id")

    def empty(self):
        return not (
            self.copy_into_copy_options
            and self.copy_into_format_options
            and self.new_cluster_spec
            and self.existing_cluster_id
        )

    def convert_format_options_to_string(self):
        return ",".join([f"'{k}' = '{v}'" for k, v in self.copy_into_format_options.items()])

    def convert_copy_options_to_string(self):
        return ",".join([f"'{k}' = '{v}'" for k, v in self.copy_into_copy_options.items()])

    @classmethod
    def get_default_delta_options(cls):
        return cls(
            copy_into_format_options={"header": "true", "inferSchema": "true"},
            copy_into_copy_options={"mergeSchema": "true"},
            load_secrets=conf.get("astro_sdk", "load_storage_configs_to_databricks", fallback=True),
        )
