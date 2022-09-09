.. _get_file_list:

=============
get_file_list
=============

When to use the ``get_file_list`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can use ``get_file_list`` to retrieve a list of available files based on a storage path and the Airflow connection. Based on the files available on your system storage, this can generate tasks dynamically.

The supported filesystems are :ref:`file_location`

.. warning::
    Fetching a lot of files using this method can lead to overloaded XCOM. This can create lot of parallel tasks when used in dynamic task map ``expand`` method.

The following example retrieves a file list from the GCS bucket and dynamically generates tasks using ``expand`` to upload each listed file to a Bigquery table.

.. literalinclude:: ../../../../example_dags/example_dynamic_task_template.py
   :language: python
   :start-after: [START howto_operator_get_file_list]
   :end-before: [END howto_operator_get_file_list]

Related references
~~~~~~~~~~~~~~~~~~

- `Dynamic task mapping - Apache Airflow <https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html>`_
- `Dynamic tasks - Astronomer <https://www.astronomer.io/guides/dynamic-tasks/>`_
