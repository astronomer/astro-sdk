.. _get_file_list:

=============
get_file_list
=============

When to use the ``get_file_list`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can use ``get_file_list``to retrieve a list of available files based on a storage path and the Airflow connection. Based on the files available on your system storage, this can generate tasks dynamically.

The supported filesystems are :ref:`file_location`

The following example retrieves a file list from the GCS bucket and dynamically generates tasks using ``expand`` to upload each listed file to a Bigquery table.

.. literalinclude:: ../../../../example_dags/example_dynamic_task_template.py
   :language: python
   :start-after: [START howto_operator_get_file_list]
   :end-before: [END howto_operator_get_file_list]
