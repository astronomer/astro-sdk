from typing import Dict, List, Optional

from airflow.models.taskinstance import TaskInstance
from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.airflow.utils import get_job_name
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Dataset as OpenlineageDataset


class OpenLineageFacets:
    def __init__(
        self,
        inputs: List[OpenlineageDataset],
        outputs: List[OpenlineageDataset],
        run_facets: Dict[str, BaseFacet],
        job_facets: Dict[str, BaseFacet],
    ):
        self.inputs = inputs
        self.outputs = outputs
        self.run_facets = run_facets
        self.job_facets = job_facets


class PythonSDKExtractor(BaseExtractor):
    """
    This extractor provides visibility on what different python-sdk operator does by
    extracting operator specific facets by calling get_openlineage_facets on each
    operator
    """

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["LoadFileOperator"]

    def extract(self) -> Optional[TaskMetadata]:  # skipcq: PYL-R0201
        """Empty extract implementation for the abstractmethod of the ``BaseExtractor`` class."""
        return None

    def extract_on_complete(
        self, task_instance: TaskInstance
    ) -> Optional[TaskMetadata]:  # skipcq: PYL-R0201, PYL-W0613
        """
        Callback on ``get_openlineage_facets(ti)`` task completion to fetch metadata extraction details that are to be
        pushed to the Lineage server.
        """
        open_lineage_facets: OpenLineageFacets = self.operator.get_openlineage_facets()

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=open_lineage_facets.inputs,
            outputs=open_lineage_facets.outputs,
            run_facets=open_lineage_facets.run_facets,
            job_facets=open_lineage_facets.job_facets,
        )
