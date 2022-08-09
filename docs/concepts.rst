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
       :start-after: [START named_table_example]
       :end-before: [END named_table_example]

#. **Temporary Tables**

    It is a common pattern to create intermediate tables during a workflow that don't need to be persisted afterwards. To accomplish this, users can use Temporary Tables in conjunction with the :ref:`cleanup_operator` task.

    There are two approaches to create temporary tables:

    #. Explicit: instantiate a ``astro.sql.table.Table`` using the argument  `temp=True`
    #. Implicit: instantiate a ``astro.sql.table.Table`` without giving it a name, and without specifying the `temp` argument

        .. literalinclude:: ../example_dags/example_amazon_s3_postgres.py
           :language: python
           :start-after: [START temp_table_example]
           :end-before: [END temp_table_example]

.. _load_file_working:


How load_file Works
~~~~~~~~~~~~~~~~~~~
.. figure:: /images/defaultPath.png


When we load a file located in cloud storage to a cloud database, internally the steps involved are:

Steps:

#. Get the file data in chunks from file storage to the worker node.
#. Send data to the cloud database from the worker node.

This is the default way of loading data into a table. There are performance bottlenecks because of limitations of memory, processing power, and internet bandwidth of worker node.


Improving bottlenecks by using native transfer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. figure:: /images/nativePath.png

Some of the cloud databases like Bigquery and Snowflake support native transfer (complete list of supported native transfers :ref:`supported_native_path`) to ingest data from cloud storage directly. Using this we can ingest data much quicker and without any involvement of the worker node.

Steps:

#. Request destination database to ingest data from the file source.
#. Database request file source for data.

This is a faster way for datasets of larger size as there is only one network call involved and usually the bandwidth between vendors is high. Also, there is no requirement for memory/processing power of the worker node, since data never gets on the node. There is significant performance improvement due to native transfers as evident from `benchmarking results <https://github.com/astronomer/astro-sdk/blob/main/tests/benchmark/results.md>`_.

**Note** - By default the native transfer is enabled and will be used if the source and destination support it, this behavior can be altered by the ``use_native_support`` param.
