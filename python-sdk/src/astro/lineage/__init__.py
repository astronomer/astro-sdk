import logging

log = logging.getLogger(__name__)

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
    logging.debug("apache-airflow-providers-openlineage python dependency is missing")
