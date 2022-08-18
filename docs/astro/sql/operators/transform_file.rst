.. _transform_file_operator:

=======================
transform_file operator
=======================

When to use the ``transform_file`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``transform_file`` operator allows you to implement the **T** of an ELT system by running a SQL query from specified SQL file. Each step of the transform pipeline creates a new table from the ``SELECT`` statement and enables tasks to pass those tables as if they were native Python objects.

The ``transform_file`` functions return a ``Table`` object that can be passed to future tasks. This table will be either an auto-generated temporary table, or will overwrite a table given in the ``output_table`` parameter. The ``transform_file`` operator treats values in the double brackets as Airflow jinja templates. You can find more details on templating at :ref:`templating`.

    .. literalinclude:: ../../../../example_dags/example_transform_file.py
       :language: python
       :start-after: [START transform_file_example_1]
       :end-before: [END transform_file_example_1]
