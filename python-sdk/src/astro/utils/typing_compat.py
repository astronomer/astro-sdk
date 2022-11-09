from typing import Any, MutableMapping

# The ``airflow.utils.context.Context`` class was not available in Apache Airflow until 2.3.3. This class is
# typically used as the typing for the ``context`` arg in operators and sensors. However, using this class for
# typing outside of TYPE_CHECKING in modules sets an implicit, minimum requirement for Apache Airflow 2.2.3
# which is currently more recent than the current minimum requirement of Apache Airflow 2.2.0.
#
# TODO: Remove this once the repo has a minimum Apache Airflow requirement of 2.2.3+.
try:
    from airflow.utils.context import Context
except ModuleNotFoundError:  # pragma: no cover

    class Context(MutableMapping[str, Any]):  # type: ignore[no-redef] # skipcq PYL-W0223
        """Placeholder typing class for ``airflow.utils.context.Context``."""


__all__ = ["Context"]
