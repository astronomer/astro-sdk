.. dataframe_operator:

======================================
dataframe operator
======================================

When to use the ``dataframe`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``dataframe`` operator allows you to run Python transformations in Airflow. Behind the scenes, the ``dataframe`` function automatically coverts the source SQL table into a Pandas dataframe, and makes any dataframes resulting from the transformation available to downstream astro.sql functions. This means you can seamlessly transition between Python and SQL for data transformations without writing any code to explicitly do so. To use the ``dataframe`` operator, you simply provide a Python function that takes a dataframe as one of its inputs, and specify a ``Table`` object as the input SQL table. If you want the resulting dataframe to be converted back to SQL, you can specify an ``output_table`` object.

There are two main uses for the ``dataframe`` operator.

Case 1: Convert a SQL table into a dataframe.

    .. literalinclude:: ../../../../example_dags/example_amazon_s3_snowflake_transform.py
       :language: python
       :start-after: [START dataframe_example_1]
       :end-before: [END dataframe_example_1]

Case 2: Convert the resulting dataframe into a table. When the ``output_table`` parameter is specified, the resulting dataframe is turned into a table.

    .. literalinclude:: ../../../../example_dags/example_amazon_s3_snowflake_transform.py
           :language: python
           :start-after: [START dataframe_example_2]
           :end-before: [END dataframe_example_2]
