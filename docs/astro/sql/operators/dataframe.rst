======================================
dataframe operator
======================================

.. _dataframe_operator:

When to use the ``dataframe`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
``dataframe`` decorator allows a user to run python functions in Airflow but with the huge benefit that SQL tables will automatically be turned into dataframes and resulting dataframes can automatically used in astro.sql functions. ``dataframe`` operator converts a SQL table into a dataframe. Users can give a python function that takes a dataframe as one of its inputs and run that python function. Once that function has completed, the result is accessible via the Taskflow API.

Example:
    The following example aggregate the data and returns dataframe.

    .. literalinclude:: ../../../../example_dags/example_amazon_s3_snowflake_transform.py
       :language: python
       :start-after: [START transform_example_1]
       :end-before: [END transform_example_1]
