import logging
import os

from airflow.configuration import conf

log = logging.getLogger(__name__)


def _is_disabled() -> bool:
    return (
        conf.getboolean("openlineage", "disabled", fallback=False)
        or os.getenv("OPENLINEAGE_DISABLED", "false").lower() == "true"
        or (
            conf.get("openlineage", "transport", fallback="") == ""
            and conf.get("openlineage", "config_path", fallback="") == ""
            and os.getenv("OPENLINEAGE_URL", "") == ""
            and os.getenv("OPENLINEAGE_CONFIG", "") == ""
        )
    )


try:
    from airflow.providers.openlineage.extractors import OperatorLineage
    from openlineage.client.facet import (
        BaseFacet,
        DataQualityMetricsInputDatasetFacet,
        DataSourceDatasetFacet,
        OutputStatisticsOutputDatasetFacet,
        SchemaDatasetFacet,
        SchemaField,
        SourceCodeJobFacet,
        SqlJobFacet,
    )
    from openlineage.client.run import Dataset as OpenlineageDataset
except ImportError:
    if not _is_disabled():
        logging.error("apache-airflow-providers-openlineage python dependency is missing")
