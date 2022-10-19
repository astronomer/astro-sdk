.. _xcom_backend:

============
XCom Backend
============

The custom XCom backend adds special handling to Astro's custom constructs (see :ref:`concepts`) so they can
be used without enabling XCom picking (the ``xcom_pickling`` configuration). When the custom constructs are
not accessed, this is simply a wrapper around Airflow's default XCom backend, so a migration from the default
backend is seamless and fully compatibility.

.. seealso::

    `Airflow documentation on custom XCom backends <https://airflow.apache.org/docs/apache-airflow/stable/concepts/xcoms.html>`__


Configure the backend
=====================

To use Astro's custom XCom backend, set the ``[core] xcom_backend`` configuration like this:

.. code-block:: ini

    [core]
    xcom_backend = astro.custom_backend.astro_custom_backend.AstroCustomXcomBackend

The same may also be achieved by setting the environment variable ``AIRFLOW__CORE__XCOM_BACKEND``.

You should also tell the backend where to store data, by providing a storage URL
and connection ID:

.. code-block:: ini

    [astro_sdk]
    xcom_storage_url = <url here>
    xcom_storage_conn_id = <conn id here>

The same may also be achieved by setting the environment variables ``AIRFLOW__ASTRO_SDK__XCOM_STORAGE_URL``
and ``AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID``.

If you don't have a storage (in local development scenarios, for example), the data can be stored in Airflow's
metadatabase instead, by setting

.. code-block:: ini

    [astro_sdk]
    store_data_local_dev = true

or the environment variable ``AIRFLOW__ASTRO_SDK__STORE_DATA_LOCAL_DEV`` instead. Note that this is considered
suboptimal, and should not be used in a production environment.
