======================================
transform operator
======================================

.. _transform_operator:

When to use the ``transform`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The transform function of the SQL decorator is the ``T`` of the ELT system. Each step of the transform pipeline creates a new table from the ``SELECT`` statement and enables tasks to pass those tables as if they were native Python objects. The transform: applies a SQL select statement to a source table and saves the result to a destination table.


There are two use cases of the ``transform`` operator.

#. When we pass tables between tasks while completing a data transformation.
#. When we pass a Pandas dataframe while completing a data transformation.

Case 1: When tables are passed
    The following example applies a SQL ``SELECT`` statement to a ``imdb_movies`` table with templating and saves the result to a ``top_animation`` table.

    .. literalinclude:: ../../../../example_dags/example_transform.py
       :language: python
       :start-after: [START transform_example_1]
       :end-before: [END transform_example_1]

    The following example applies a SQL ``SELECT`` statement to a ``imdb_movies`` table with templating and saves the result to a ``last_animation`` table.

    .. literalinclude:: ../../../../example_dags/example_transform.py
       :language: python
       :start-after: [START transform_example_2]
       :end-before: [END transform_example_2]

    We can quickly pass tables between tasks when completing a data transformation.

    .. literalinclude:: ../../../../example_dags/example_transform.py
       :language: python
       :start-after: [START transform_example_3]
       :end-before: [END transform_example_3]

Case 2: When Pandas dataframe is passed
    The following example DAG shows how we can quickly pass table and pandas dataframe between tasks when completing a data transformation.

    .. literalinclude:: ../../../../example_dags/example_transform.py
       :language: python
       :start-after: [START transform_example_4]
       :end-before: [END transform_example_4]
