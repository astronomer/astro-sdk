.. _run_raw_sql:

============================================================
:py:mod:`run_raw_sql operator <astro.sql.operators.raw_sql>`
============================================================

When to use the ``run_raw_sql`` operator
-----------------------------------------
The ``run_raw_sql`` operator allows you to declare any SQL statement using the Astro SDK :ref:`templating` available in :ref:`transform_operator`. By default this operator returns ``None``, but you can alternatively define the task output by using the ``handler`` argument. For example, you may wish to return a list with the row results of a query.

The ``run_raw_sql`` function also treats values in double brackets as Airflow jinja templates. You can find more details on templating at :ref:`templating`.

This example shows how you can create a Snowflake table using ``run_raw_sql`` without the ``handler``. This task will return ``None``.

.. literalinclude:: ../../../../example_dags/example_snowflake_partial_table_with_append.py
    :language: python
    :start-after: [START howto_run_raw_sql_snowflake_1]
    :end-before: [END howto_run_raw_sql_snowflake_1]


This example shows how you can run a ``select`` query in Bigquery and return rows using the ``handler`` argument. This task will return the results of the query.

.. literalinclude:: ../../../../example_dags/example_bigquery_dynamic_map_task.py
    :language: python
    :start-after: [START howto_run_raw_sql_with_handle_1]
    :end-before: [END howto_run_raw_sql_with_handle_1]

Parameters
-----------
* **handler** - This parameter is used to pass a callback and this callback gets a cursor object from the database.

* **results_format** - There are common scenarios where the kind of results you would expect from the handler function to return.

    #. List - If you expect a query return to be a list of rows. instead of passing handler to do ``cursor.fetchall()``, we can pass ``results_format=='list'``

    #. Pandas Dataframe - If you expect query result to be converted to ``Pandas Dataframe`` we can pass ``results_format=='pandas_dataframe'``

* **fail_on_empty** - Sometimes the handler function can raise an exception when the data is not returned by the database and we try to run ``fetchall()``. We can make sure that the handler function doesn't raise an exception by passing ``fail_on_empty==False``. The default value for this parameter is ``True``.

* **query_modifier** - The query_modifier allows you to create both pre_queries and post_queries across multiple statements. An example of where this would be useful is if you want to add query tags to a snowflake statement you can set ``session_modifier.pre_queries = ["ALTER SESSION SET QUERY_TAG=<my-query-tag>]``, which will ensure that any query run will contain this query tag.
