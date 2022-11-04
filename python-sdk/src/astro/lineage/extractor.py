from __future__ import annotations

import attr
from airflow.models.taskinstance import TaskInstance
from openlineage.airflow.extractors import TaskMetadata
from openlineage.airflow.extractors.base import BaseExtractor
from openlineage.airflow.utils import get_job_name
from openlineage.client.facet import BaseFacet
from openlineage.client.run import Dataset as OpenlineageDataset


@attr.define
class OpenLineageFacets:
    """
    OpenLineageFacets are pieces of metadata that can be attached to the core entities: Run,
    Job and Dataset as per https://github.com/OpenLineage/OpenLineage/blob/main/spec/OpenLineage.md#facets
    """

    inputs: list[OpenlineageDataset]
    outputs: list[OpenlineageDataset]
    run_facets: dict[str, BaseFacet]
    job_facets: dict[str, BaseFacet]


class PythonSDKExtractor(BaseExtractor):
    """
    This extractor provides visibility on what different python-sdk operator does by
    extracting operator specific facets by calling get_openlineage_facets on each
    operator
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return [
            "AppendOperator",
            "BaseSQLDecoratedOperator",
            "DataframeOperator",
            "ExportFileOperator",
            "LoadFileOperator",
            "MergeOperator",
            "TransformOperator",
        ]

    def extract(self) -> TaskMetadata:
        """Empty extract implementation for the abstractmethod of the ``BaseExtractor`` class."""
        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=[],
            outputs=[],
            run_facets={},
            job_facets={},
        )

    def extract_on_complete(
        self, task_instance: TaskInstance  # skipcq: PYL-W0613,  PYL-R0201
    ) -> TaskMetadata | None:  # skipcq: PYL-R0201, PYL-W0613
        """
        Callback on ``get_openlineage_facets(ti)`` task completion to fetch metadata extraction details that are to be
        pushed to the Lineage server.
        """
        open_lineage_facets: OpenLineageFacets = self.operator.get_openlineage_facets(task_instance)

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=open_lineage_facets.inputs,
            outputs=open_lineage_facets.outputs,
            run_facets=open_lineage_facets.run_facets,
            job_facets=open_lineage_facets.job_facets,
        )
