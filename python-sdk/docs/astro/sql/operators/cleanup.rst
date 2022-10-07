======================================
cleanup operator
======================================

.. _cleanup_operator:

When to use the ``cleanup`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``cleanup`` operator allows you to clean up temporary tables(:ref:`table`) created as part of your Astro SDK pipeline. The ``cleanup`` function monitors the status of all the tasks within your DAG, and deletes any temporary tables(:ref:`table`) at the end of the DAG run. Astronomer recommends adding a ``cleanup`` operator to every DAG that uses the Astro SDK.

.. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_amazon_s3_postgres.py
   :language: python
   :start-after: [START cleanup_example]
   :end-before: [END cleanup_example]

You can also specify the temporary tables you want to delete by providing a list of tables with the parameter ``tables_to_cleanup``. Only temporary tables provided in this list will be deleted; persistent tables will be ignored.
