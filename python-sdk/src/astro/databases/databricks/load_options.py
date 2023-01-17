from __future__ import annotations

from airflow.configuration import conf
from attr import define, field

from astro.constants import DatabricksLoadMode, LoadExistStrategy
from astro.options import LoadOptions


@define
class DeltaLoadOptions(LoadOptions):
    """
    Helper class for rendering options into COPY_INTO Databricks SQL statements

    :param copy_into_format_options: File format options for COPY INTO. Read more at:
        https://docs.databricks.com/sql/language-manual/delta-copy-into.html#format-options
    :param copy_into_copy_options: Options to control the operation of the COPY INTO command. Read more at:
        https://docs.databricks.com/sql/language-manual/delta-copy-into.html
    :param existing_cluster_id: Existing cluster ID on databricks.
    :param new_cluster_spec: New cluster specifications.
    :param if_exists: default override an existing Table. Options: fail, replace, append
    :param secret_scope: Secrets scope
    :param load_secrets: Defaults to False.
    :param load_mode: defaults to autoloader. Options: autoloader, COPY INTO
    :param autoloader_load_options: Options for load using autoloader. Read more at:
        https://docs.databricks.com/ingestion/auto-loader/options.html
    """

    copy_into_format_options: dict = field(init=True, factory=dict)
    copy_into_copy_options: dict = field(init=True, factory=dict)
    existing_cluster_id: str = field()
    new_cluster_spec: dict = field(init=True, factory=dict)
    if_exists: LoadExistStrategy = "replace"
    secret_scope: str = "astro-sdk-secrets"
    load_secrets: bool = False
    load_mode: DatabricksLoadMode = DatabricksLoadMode.AUTOLOADER
    autoloader_load_options: dict = field(factory=dict)

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
