.. _run_raw_sql:

====================
run_raw_sql operator
====================

When to use the ``run_raw_sql`` operator
-----------------------------------------
The ``run_raw_sql`` operator allows you to declare any SQL statement using the Astro SDK :ref:`templating` available in :ref:`transform_operator`. By default this operator returns ``None``, but you can alternatively define the task output by using the ``handler`` argument. For example, you may wish to return a list with the row results of a query.

The ``run_raw_sql`` function also treats values in double brackets as Airflow jinja templates. You can find more details on templating at :ref:`templating`.

This example shows how you can create a Snowflake table using ``run_raw_sql`` without the ``handler``. This task will return ``None``.

.. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_snowflake_partial_table_with_append.py
    :language: python
    :start-after: [START howto_run_raw_sql_snowflake_1]
    :end-before: [END howto_run_raw_sql_snowflake_1]


This example shows how you can run a ``select`` query in Bigquery and return rows using the ``handler`` argument. This task will return the results of the query.

.. literalinclude:: ../../../../example_dags/2.4/2.3/example_bigquery_dynamic_map_task.py
    :language: python
    :start-after: [START howto_run_raw_sql_with_handle_1]
    :end-before: [END howto_run_raw_sql_with_handle_1]
