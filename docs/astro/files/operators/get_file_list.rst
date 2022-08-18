.. _get_file_list:

=============
get_file_list
=============

When to use the ``get_file_list`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We can use ``get_file_list`` when you want a list file available in storage based on storage path pattern and Airflow connection. This might useful when you want to create dynamic task on the response of this operator.

The supported filesystem are :ref:`file_location`

The below example, get a file list from the GCS bucket and upload them to the Bigquery table by creating a dynamic number of parallel tasks using the dynamic task map ``expand`` method.

.. literalinclude:: ../../../../example_dags/example_dynamic_task_template.py
   :language: python
   :start-after: [START howto_operator_get_file_list]
   :end-before: [END howto_operator_get_file_list]
