from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.models.xcom import BaseXCom

from astro.custom_backend.serializer import deserialize, serialize

if TYPE_CHECKING:
    from airflow.models.xcom import XCom


class AstroCustomXcomBackend(BaseXCom):
    """
    The Astro custom xcom backend is an xcom custom backend wrapper that handles serialization and deserialization
    of command data types used in astro. The astro library overrides the ``TaskInstance.XCom`` object with this wrapper.

    Once the wrapper has properly serialized the objects, the json-safe dictionaries are then passed to the original
    XCom object. This ensures that custom backends still work and there are no breaking changes to airflow users.
    """

    @staticmethod
    def serialize_value(value: Any, **kwargs) -> Any:  # type: ignore[override]
        """
        Serialize the return value of a taskinstance and then pass it to the XCom.serialize_value function.
        :param value: object to be serialized.
        :param kwargs:
        :return:
        """
        from astro.settings import DATAFRAME_STORAGE_CONN_ID, STORE_DATA_LOCAL_DEV

        if DATAFRAME_STORAGE_CONN_ID or STORE_DATA_LOCAL_DEV:
            value = serialize(value)
        else:
            raise AirflowException(
                "Since you have not provided a remote object storage conn_id, we would need to store your dataframes"
                " in the Metadata DB. This does not scale well and can cause degradation to "
                "your airflow DB. Please set the AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID and "
                "AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL variables "
                "(or you can set AIRFLOW__ASTRO_SDK__STORE_DATA_LOCAL_DEV for local development)"
            )
        return BaseXCom.serialize_value(value, **kwargs)

    @staticmethod
    def deserialize_value(result: "XCom") -> Any:
        """
        Deserializing the result of a xcom_pull before passing th result to the next task.
        :param result:
        :return:
        """
        result = BaseXCom.deserialize_value(result)
        return deserialize(result)
