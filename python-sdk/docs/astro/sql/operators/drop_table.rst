.. _drop_table_operator:

======================================
drop table operator
======================================

When to use the ``drop_table`` operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The ``drop_table`` operator allows you to delete tables from your database. It can be used on both temporary as well as persistent tables.

.. literalinclude:: ../../../../example_dags/2.4/2.3/2.2.5/example_sqlite_load_transform.py
   :language: python
   :start-after: [START drop_table_example]
   :end-before: [END drop_table_example]
