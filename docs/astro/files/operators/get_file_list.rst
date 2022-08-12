.. _get_file_list:

=============
get_file_list
=============

When to use the ``get_file_list`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We can use ``get_file_list`` when you want a list file available in storage based on storage path pattern and Airflow connection. This might useful when you want to create dynamic task on the response of this operator.

.. literalinclude:: ../../../../example_dags/example_dynamic_task_template.py
   :language: python
   :start-after: [START howto_operator_get_file_list]
   :end-before: [END howto_operator_get_file_list]
