============
Concepts
============

.. _named_table:

Named Tables
~~~~~~~~~~~~~

These are tables that are of some importance to users and will we persist in a database even after a DAG run is finished. You can create these tables by passing in a ``name`` parameter while creating a ``astro.sql.table.Table`` object.

.. literalinclude:: ../example_dags/example_amazon_s3_postgres_load_and_save.py
   :language: python
   :start-after: [named_table_example_start]
   :end-before: [named_table_example_end]


.. _temp_table:

Temporary Tables
~~~~~~~~~~~~~~~~~~~

These are the tables that only exist while a dag is running and will be deleted from the database post that. There is an assumption that these tables are not considered important to the user and only hold intermediate data. You can create these by not passing the ``name`` parameter while creating a ``astro.sql.table.Table`` object.

.. literalinclude:: ../example_dags/example_amazon_s3_postgres.py
   :language: python
   :start-after: [temp_table_example_start]
   :end-before: [temp_table_example_end]
