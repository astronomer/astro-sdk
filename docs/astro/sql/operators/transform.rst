======================================
transform operator
======================================

.. _transform_operator:

When to use the ``transform`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The transform function of the SQL decorator is the ``T`` of the ELT system. Each step of the transform pipeline creates a new table from the ``SELECT`` statement and enables tasks to pass those tables as if they were native Python objects.

Example 1:
    The following example applies a SQL ``SELECT`` statement to a ``imdb_movies`` table and saves the result to a ``top_animation`` table.

    .. literalinclude:: ../../../../example_dags/example_sqlite_load_transform.py
       :language: python
       :start-after: [START transform_example_1]
       :end-before: [END transform_example_1]

Example 2:
    The following example DAG shows how we can quickly pass tables between tasks when completing a data transformation.

    .. literalinclude:: ../../../../example_dags/example_amazon_s3_snowflake_transform.py
       :language: python
       :start-after: [START transform_example_2]
       :end-before: [END transform_example_2]
