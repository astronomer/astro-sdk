======================================
cleanup operator
======================================

.. _cleanup_operator:

When to use the ``cleanup`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``cleanup`` operator is used to clean temporary tables(:ref:`table`). It monitors the status of all the tasks within a DAG and deletes the created temporary tables(:ref:`table`) by the end of the DAG run. It is recommended to add ``cleanup`` operator in every DAG that uses the Astro SDK.

.. literalinclude:: ../../../../example_dags/example_amazon_s3_postgres.py
   :language: python
   :start-after: [START cleanup_example]
   :end-before: [END cleanup_example]
