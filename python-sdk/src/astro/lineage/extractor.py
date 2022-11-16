from __future__ import annotations

from airflow.models.taskinstance import TaskInstance

from astro.lineage import BaseExtractor, TaskMetadata, get_job_name, OperatorLineage


class PythonSDKExtractor(BaseExtractor):
    """
    This extractor provides visibility on what different python-sdk operator does by
    extracting operator specific facets by calling get_openlineage_facets on each
    operator. Due to the bug https://github.com/OpenLineage/OpenLineage/issues/1256
    we have the above class for getting lineage for AppendOperator, MergeOperator,
    DataframeOperator and BaseSQLDecoratedOperator.
    TODO once the above mentioned bug gets released we need to remove this.
    """

    @classmethod
    def get_operator_classnames(cls) -> list[str]:
        return [
            "AppendOperator",
            "BaseSQLDecoratedOperator",
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
        open_lineage_facets: OperatorLineage = self.operator.get_openlineage_facets(task_instance)

        return TaskMetadata(
            name=get_job_name(task=self.operator),
            inputs=open_lineage_facets.inputs,
            outputs=open_lineage_facets.outputs,
            run_facets=open_lineage_facets.run_facets,
            job_facets=open_lineage_facets.job_facets,
        )
