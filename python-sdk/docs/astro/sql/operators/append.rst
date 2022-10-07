.. _append_operator:

================
append operator
================
When to use the ``append`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``append`` operator allows you to append data from a source table to a target table.

.. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_append.py
   :language: python
   :start-after: [START append_example]
   :end-before: [END append_example]

If a columns parameter is not provided, the ``append`` operator assumes that the source and target tables have the same schema.

Tables have the same schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. **Case 1:** When the complete table needs to be appended, the ``columns`` parameter can be omitted.
    .. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_append.py
       :language: python
       :start-after: [START append_example]
       :end-before: [END append_example]

#. **Case 2:** When only a subset of columns needs to be appended, you can pass a ``list`` of columns to the ``columns`` parameter.
    .. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_snowflake_partial_table_with_append.py
       :language: python
       :start-after: [START append_example_with_columns_list]
       :end-before: [END append_example_with_columns_list]

Tables have different schemas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
When the source and target tables have different schemas, you can map different column names by passing a ``dict`` of *source columns to target columns*.

    .. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_append.py
       :language: python
       :start-after: [START append_example_col_dict]
       :end-before: [END append_example_col_dict]

Conflicts
~~~~~~~~~
The ``append`` operator can't handle conflicts that may arise while appending data. If you want to handle those scenarios, you can use the :ref:`merge_operator`.

Default Datasets
~~~~~~~~~~~~~~~~
* Input dataset - Source table for the operator.
* Output dataset - Target table of the operator.
