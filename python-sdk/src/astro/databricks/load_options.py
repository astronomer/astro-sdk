from __future__ import annotations

from airflow.configuration import conf
from attr import define, field

from astro.options import LoadOptions


@define
class DeltaLoadOptions(LoadOptions):
    """Helper class for rendering options into COPY_INTO Databricks SQL statements"""

    copy_into_format_options: dict = field(init=True, factory=dict)
    copy_into_copy_options: dict = field(init=True, factory=dict)
    existing_cluster_id: str = conf.get("astro_sdk", "databricks_cluster_id")
    new_cluster_spec: dict = field(init=True, factory=dict)

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


default_delta_options = DeltaLoadOptions(
    copy_into_format_options={"header": "true", "inferSchema": "true"},
    copy_into_copy_options={"mergeSchema": "true"},
    existing_cluster_id=conf.get("astro_sdk", "databricks_cluster_id"),
)
