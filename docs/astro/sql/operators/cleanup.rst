======================================
cleanup operator
======================================

.. _cleanup_operator:

When to use the ``cleanup`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Cleanup operator is used to clean temporary tables(:ref:`table`) created. It monitors the status of all the other tasks and execute at the end to clear all the temporary tables(:ref:`table`) created in a dag run. It is recommended to add ``cleanup`` operator in every DAG that uses the Astro SDK.

.. literalinclude:: ../../../../example_dags/example_amazon_s3_postgres.py
   :language: python
   :start-after: [START cleanup_example]
   :end-before: [END cleanup_example]
