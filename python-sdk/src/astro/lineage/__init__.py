import logging

log = logging.getLogger(__name__)

try:
    from openlineage.airflow.extractors import TaskMetadata
    from openlineage.airflow.extractors.base import BaseExtractor, OperatorLineage
    from openlineage.airflow.utils import get_job_name
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
    logging.debug("openlineage-airflow python dependency is missing")
