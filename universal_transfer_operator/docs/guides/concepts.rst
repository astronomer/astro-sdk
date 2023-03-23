.. _concepts:

========
Concepts
========

.. _dataset:

Datasets
--------

A dataset in Airflow represents a logical collection of data. Upstream tasks that produce data can update datasets, and downstream DAGs that consume data can be scheduled based on dataset updates.

A dataset is defined by a Uniform Resource Identifier (URI):

.. code-block:: python

    from airflow import Dataset

    example_dataset = Dataset("s3://dataset-bucket/example.csv")


The Universal Transfer Operator supports two :ref:`dataset` specializations: :ref:`table` and :ref:`file`, which enrich the original Airflow :ref:`dataset` concept, for instance, by allowing uses to associate Airflow connections to them. Examples:

#. :ref:`table` as a Dataset

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
           :language: python
           :start-after: [START dataset_table]
           :end-before: [END dataset_table]

#. :ref:`file` as a Dataset

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
           :language: python
           :start-after: [START dataset_file]
           :end-before: [END dataset_file]

#. :ref:`api` as a Dataset

#. :ref:`dataframe` as a Dataset

.. _table:

Tables
------

A Table Dataset refers to a table in a database or a data warehouse like Postgres, Snowflake, etc. that stores data. They can be used in Universal Transfer Operator as input dataset or output datasets.

There are two types of tables:

#. **Persistent Table**

    These are tables that are of some importance to users.  You can create these tables by passing in a ``name`` parameter while creating a ``universal_transfer_operator.datasets.table.Table`` object.

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START dataset_table]
       :end-before: [END dataset_table]

#. **Temporary Tables**

    The user can transfer data without giving the table name, and the universal transfer operator will make temporary tables that have unique names starting with ``_tmp``.

    There are two approaches to create temporary tables:

    #. Explicit: instantiate a ``universal_transfer_operator.datasets.table.Table`` using the argument  `temp=True`
    #. Implicit: instantiate a ``universal_transfer_operator.datasets.table.Table`` without giving it a name, and without specifying the `temp` argument

        .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
           :language: python
           :start-after: [START dataset_table]
           :end-before: [END dataset_table]

Metadata
~~~~~~~~
Metadata is used to give additional information to access a SQL Table.
For example, a user can detail the Google Bigquery schema and database for a table. Although these parameters can change name depending on the database, we have normalised the :class:`~universal_transfer_operator.datasets.table.Metadata` class to name their schema and database.

.. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
   :language: python
   :start-after: [START dataset_table]
   :end-before: [END dataset_table]

.. _file:

Files
-----

The File Dataset represents a file located on a storage mechanism such as S3, GCS, etc. This is a very common type of Dataset in data pipelines, often used as a source of data or as an output for analysis.

There are two types of files:

#. **File**

    These are individual files specified by the user.  You can create these file by passing in a ``path`` parameter while creating a ``universal_transfer_operator.datasets.base.File`` object.

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START dataset_individual_file]
       :end-before: [END dataset_individual_file]

#. **File pattern or folder location**

    The user can transfer data to and from folder or file pattern. We also resolve the :ref:`file_pattern` in file path based on the :ref:`file_location`. For example:

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START dataset_file]
       :end-before: [END dataset_file]

.. _dataframe:

Dataframe
---------

A DataFrame is another type of Dataset that is becoming more popular in data pipelines. It used to be a temporary processing step within a single task, but now there are more demand and technologies for storing distributed data frames, such as Ray or Snowpark.

A DataFrame Dataset is still a reference object, but its URI may not be clear yet. This is because different frameworks have different ways of representing data frames, such as Spark, Pandas, or Snowpark.


.. _api:

API
---------

An API Dataset refers to data that is obtained or published using a REST API from or to another system. Data pipelines often use this type of Dataset to get data from remote sources in JSON format.

Another type of API Dataset is used to send data to SaaS applications using a REST API. This is sometimes called “Reverse ETL” and it is an emerging operation.


.. _transfer_working:


How Universal Transfer Operator Works
-------------------------------------
.. figure:: /images/approach.png

With universal transfer operator, users can perform data transfers using the following transfer modes:

.. literalinclude:: ../../src/universal_transfer_operator/constants.py
   :language: python
   :start-after: [START TransferMode]
   :end-before: [END TransferMode]

Non-native transfer
-------------------
When we load a data located in one dataset located in cloud to another dataset located in cloud, internally the steps involved are:

Steps:

#. Get the dataset data in chunks from dataset storage to the worker node.
#. Send data to the cloud dataset from the worker node.

This is the default way of transferring datasets. There are performance bottlenecks because of limitations of memory, processing power, and internet bandwidth of worker node.

Following is an example of non-native transfers between Google cloud storage and Sqlite:

.. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
   :language: python
   :start-after: [START transfer_non_native_gs_to_sqlite]
   :end-before: [END transfer_non_native_gs_to_sqlite]


Improving bottlenecks by using native transfer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
.. figure:: /images/approach.png

Some of the datasets on cloud like Bigquery and Snowflake support native transfer to ingest data from cloud storage directly. Using this we can ingest data much quicker and without any involvement of the worker node.

Steps:

#. Request destination dataset to ingest data from the file dataset.
#. Destination dataset request source dataset for data.

This is a faster way for datasets of larger size as there is only one network call involved and usually the bandwidth between vendors is high. Also, there is no requirement for memory/processing power of the worker node, since data never gets on the node. There is significant performance improvement due to native transfers.

Transfer using third-party tool
-------------------------------
The universal transfer operator can work smoothly with other platforms like FiveTran for data transfers.

Following are the supported third-party platforms:

.. literalinclude:: ../../src/universal_transfer_operator/constants.py
   :language: python
   :start-after: [START transferingestor]
   :end-before: [END transferingestor]

Here is an example of how to use Fivetran for transfers:

.. literalinclude:: ../../example_dags/example_dag_fivetran.py
   :language: python
   :start-after: [START fivetran_transfer_with_setup]
   :end-before: [END fivetran_transfer_with_setup]

.. _file_pattern:

Patterns in File path
~~~~~~~~~~~~~~~~~~~~~

We also resolve the patterns in file path based on the :ref:`file_location`

#. **Local** - Resolves ``File.path`` using the glob standard library (https://docs.python.org/3/library/glob.html)
#. **S3** - Resolves ``File.path`` using AWS S3 prefix rules (https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-prefixes.html)
#. **GCS** - Resolves ``File.path`` using Google Cloud Storage (GCS) wildcard rules (https://cloud.google.com/storage/docs/gsutil/addlhelp/WildcardNames)
