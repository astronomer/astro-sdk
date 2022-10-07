.. _transform_operator:

==================
transform operator
==================

When to use the ``transform`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``transform`` operator allows you to implement the **T** of an ELT system by running a SQL query. Each step of the transform pipeline creates a new table from the ``SELECT`` statement and enables tasks to pass those tables as if they were native Python objects.

The ``transform`` operator treats values in the double brackets as Airflow jinja templates. You can find more details on templating at :ref:`templating`.

There are two main uses for the ``transform`` operator.

Case 1: Passing tables between tasks while completing data transformations.
    The following example applies a SQL ``SELECT`` statement to a ``imdb_movies`` table with templating and saves the result to a ``top_animation`` table.

    Note that the ``input_table`` in the double brackets is treated as an Airflow jinja template. It is **not** an f string. F-strings in SQL formatting are at risk of security breaches via SQL injections. For security, you **must** explicitly identify tables in the function parameters by typing a value as a Table. Only then will the ``transform`` operator treat the value as a table.

    .. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_transform.py
       :language: python
       :start-after: [START transform_example_1]
       :end-before: [END transform_example_1]

    The following example applies a SQL ``SELECT`` statement to a ``imdb_movies`` table with templating and saves the result to a ``last_animation`` table.

    .. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_transform.py
       :language: python
       :start-after: [START transform_example_2]
       :end-before: [END transform_example_2]

    You can easily pass tables between tasks when completing a data transformation.

    .. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_transform.py
       :language: python
       :start-after: [START transform_example_3]
       :end-before: [END transform_example_3]

Case 2: Passing a Pandas dataframe between tasks while completing data transformations.
    The following example shows how you can quickly pass a table and a Pandas dataframe between tasks when completing a data transformation.

    .. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_transform.py
       :language: python
       :start-after: [START transform_example_4]
       :end-before: [END transform_example_4]

Please note that in case you want to pass SQL file in the transform decorator, use :ref:`transform_file_operator`
