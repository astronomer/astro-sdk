.. _append_operator:

======================================
append operator
======================================

When to use the ``append`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
We can use ``append`` operator when we want to append the source table to the target table.

.. literalinclude:: ../../../../example_dags/example_append.py
   :language: python
   :start-after: [START append_example]
   :end-before: [END append_example]

When used without a columns parameter, AstroSDK assumes that both tables have the same schema.

When tables have same schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#. **Case 1:** When complete table needs to be merged. You can skip ``columns`` parameter.
    .. literalinclude:: ../../../../example_dags/example_append.py
       :language: python
       :start-after: [START append_example]
       :end-before: [END append_example]

#. **Case 2:** When subset of columns needs to be merged to target table we pass ``List`` of cols in ``columns`` parameter.
    .. literalinclude:: ../../../../example_dags/example_snowflake_partial_table_with_append.py
       :language: python
       :start-after: [START append_example_with_columns_list]
       :end-before: [END append_example_with_columns_list]

When table have different schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
When tables have different schema, we can map different column names by passing a ``dict`` of *source cols to target cols*.

    .. literalinclude:: ../../../../example_dags/example_append.py
       :language: python
       :start-after: [START append_example_col_dict]
       :end-before: [END append_example_col_dict]

Conflicts
~~~~~~~~~
``append operator`` doesn't handle the conflicts that may arise while appending data. If you want to handle those scenarios, you can use :ref:`merge_operator`.
