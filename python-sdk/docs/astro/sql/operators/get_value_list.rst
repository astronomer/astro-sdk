.. _get_value_list:

==============
get_value_list
==============

When to use the ``get_value_list`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We can use ``get_value_list`` when you want to execute a SQL query on a database table and get the result. This might be useful when you want to create dynamic tasks on the response of this operator.

.. warning::
    Using this operator without limit in the SQL query can push lots of data in XCOM and also can create lots of parallel tasks if using in dynamic task map expand method.

.. literalinclude:: ../../../../example_dags/2.4/2.3/example_dynamic_task_template.py
   :language: python
   :start-after: [START howto_operator_get_value_list]
   :end-before: [END howto_operator_get_value_list]


Related references
~~~~~~~~~~~~~~~~~~

- `Dynamic task mapping - Apache Airflow <https://airflow.apache.org/docs/apache-airflow/2.3.0/concepts/dynamic-task-mapping.html>`_
- `Dynamic tasks - Astronomer <https://www.astronomer.io/guides/dynamic-tasks/>`_
