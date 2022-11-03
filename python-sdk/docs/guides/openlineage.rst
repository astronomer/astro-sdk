.. _openlineage:

============
OpenLineage
============

OpenLineage is an open framework for data lineage collection and analysis. At its core is an extensible
specification that systems can use to interoperate with lineage metadata. Astro SDK operators needs a
wrapper around openlineage extractors to show the events on the UI.

.. seealso::

    `Enabling OpenLineage in Apache Airflow <https://openlineage.io/integration/apache-airflow/>`__


Configure the Openlineage
==========================

We'll need to specify where we want Airflow to send OpenLineage events. openlineage-airflow will use the
``OPENLINEAGE_URL`` environment variable to send OpenLineage events to Marquez. Optionally, we can also
specify a namespace where the lineage events will be stored using the ``OPENLINEAGE_NAMESPACE`` environment variable.

.. code-block:: ini

    AIRFLOW__LINEAGE__BACKEND=openlineage.lineage_backend.OpenLineageBackend
    OPENLINEAGE_URL=http://host.docker.internal:5050/
    OPENLINEAGE_NAMESPACE=astro

If you want to use the Astro SDK extractors to get openlineage events, then set the environment variable

.. code-block:: ini

    OPENLINEAGE_EXTRACTORS="astro.lineage.extractor.PythonSDKExtractor"
