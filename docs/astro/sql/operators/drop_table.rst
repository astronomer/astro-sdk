.. _drop_table_operator:

======================================
drop table operator
======================================

When to Use drop table operator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The drop table operator is used to delete tables from the database if they exist. It can be used on both Temporary as well as Persistent Tables.

.. literalinclude:: ../../../../example_dags/example_sqlite_load_transform.py
   :language: python
   :start-after: [START drop_table_example]
   :end-before: [END drop_table_example]
