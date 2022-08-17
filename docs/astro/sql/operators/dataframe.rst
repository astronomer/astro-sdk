.. dataframe_operator:

======================================
dataframe operator
======================================

When to use the ``dataframe`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``dataframe`` decorator allows a user to run python functions in Airflow but with the huge benefit that SQL tables will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions. When a ``Table`` object is passed into the``dataframe`` operator, the operator automatically converts the SQL table into a dataframe. Users can then give a python function that takes a dataframe as one of its inputs and run that python function against the rendered dataset. Once the python function has completed, the result is accessible via the Taskflow API.

There are two use cases of the ``dataframe`` decorator.

Case 1: Converts a SQL table into a dataframe.

    .. literalinclude:: ../../../../example_dags/example_amazon_s3_snowflake_transform.py
       :language: python
       :start-after: [START dataframe_example_1]
       :end-before: [END dataframe_example_1]

Case 2: Convert the resulting dataframe into a table. When ``output_table`` parameter is specified, it turns the resulting dataframe into a table.

    .. literalinclude:: ../../../../example_dags/example_amazon_s3_snowflake_transform.py
           :language: python
           :start-after: [START dataframe_example_2]
           :end-before: [END dataframe_example_2]
