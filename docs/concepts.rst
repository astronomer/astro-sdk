========
Concepts
========

.. _table:

Tables
~~~~~~~

Tables represent the location and, optionally, the column types of a SQL Database table. They are used in most Astro SDK tasks and decorators.

There are two types of tables:

#. **Persistent Table**

    These are tables that are of some importance to users and will we persist in a database even after a DAG run is finished and won't be deleted by :ref:`cleanup_operator`. Users can still drop them by using ``drop_table operator(to be replaced with ref)``. You can create these tables by passing in a ``name`` parameter while creating a ``astro.sql.table.Table`` object.

    .. literalinclude:: ../example_dags/example_amazon_s3_postgres_load_and_save.py
       :language: python
       :start-after: [named_table_example_start]
       :end-before: [named_table_example_end]

#. **Temporary Tables**

    It is a common pattern to create intermediate tables during a workflow that don't need to be persisted afterwards. To accomplish this, users can use Temporary Tables in conjunction with the :ref:`cleanup_operator` task.

    There are two approaches to create temporary tables:

    #. Explicit: instantiate a ``astro.sql.table.Table`` using the argument  `temp=True`
    #. Implicit: instantiate a ``astro.sql.table.Table`` without giving it a name, and without specifying the `temp` argument

        .. literalinclude:: ../example_dags/example_amazon_s3_postgres.py
           :language: python
           :start-after: [temp_table_example_start]
           :end-before: [temp_table_example_end]
