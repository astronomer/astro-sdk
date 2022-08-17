.. _get_value_list:

==============
get_value_list
==============

When to use the ``get_value_list`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We can use ``get_value_list`` when you want to execute a SQL query on a database table and get the result. This might be useful when you want to create dynamic tasks on the response of this operator.

.. literalinclude:: ../../../../example_dags/example_dynamic_task_template.py
   :language: python
   :start-after: [START howto_operator_get_value_list]
   :end-before: [END howto_operator_get_value_list]
