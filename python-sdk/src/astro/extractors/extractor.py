from typing import List, Optional

from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.airflow.utils import get_job_name


class PythonSDKExtractor(BaseExtractor):
    """
    This extractor provides visibility on what different python-sdk operator does by
    extracting operator specific facets by calling get_openlineage_facets on each
    operator
    """

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ["LoadFileOperator"]

    def extract(self) -> Optional[TaskMetadata]:
        """Empty extract implementation for the abstractmethod of the ``BaseExtractor`` class."""
        return None

    def extract_on_complete(self) -> Optional[TaskMetadata]:  # skipcq: PYL-R0201
        """
        Callback on ``get_openlineage_facets(ti)`` task completion to fetch metadata extraction details that are to be
        pushed to the Lineage server.
        """
        try:
            input_dataset, output_dataset = self.operator.get_openlineage_facets()
        except ValueError:
            return TaskMetadata(
                name=get_job_name(task=self.operator),
                inputs=[],
                outputs=[],
            )

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=input_dataset,
            outputs=output_dataset,
        )
