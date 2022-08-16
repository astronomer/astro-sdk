.. _run_raw_sql:

====================
run_raw_sql operator
====================

When to use the ``run_raw_sql`` operator
-----------------------------------------
It allows users to declare any SQL statement using the Astro SDK :ref:`templating` available in :ref:`transform_operator`. By default this operator returns ``None`` but it also empowers users to define the task output (e.g. a list with the row results, etc) by using the ``handler`` argument.

The ``run_raw_sql`` function also treats values in the double brackets as Airflow jinja templates. More details on templating can be found at :ref:`templating`.

Create Snowflake table using ``run_raw_sql`` without ``handler`` argument i.e this will return ``None``

.. literalinclude:: ../../../../example_dags/example_snowflake_partial_table_with_append.py
    :language: python
    :start-after: [START howto_run_raw_sql_snowflake_1]
    :end-before: [END howto_run_raw_sql_snowflake_1]


Run ``select`` query in Bigquery using ``run_raw_sql`` decorator and return rows using ``handler`` argument.

.. literalinclude:: ../../../../example_dags/example_bigquery_dynamic_map_task.py
    :language: python
    :start-after: [START howto_run_raw_sql_with_handle_1]
    :end-before: [END howto_run_raw_sql_with_handle_1]
