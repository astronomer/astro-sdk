***************
Sqlite
***************

Transfer to Sqlite as destination dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data to Sqlite as destination as from following sources dataset:

#. :ref:`table`
    - Sqlite
    - Snowflake
    - Bigquery

#. :ref:`file`
    - AWS S3
    - Google cloud storage
    - Local
    - SFTP

Following transfer modes are supported:

1. Transfer using non-native approach
    Following is an example of non-native transfers between google bigquery to Sqlite using non-native transfers:

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_bigquery_to_sqlite]
       :end-before: [END transfer_non_native_bigquery_to_sqlite]

2. Transfer using native approach
3. Transfer using third-party platform

Examples
########
1. Google Bigquery to Sqlite transfers
    - Non-native
        Following is an example of non-native transfers between Google Bigquery to Sqlite using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_sqlite]
               :end-before: [END transfer_non_native_bigquery_to_sqlite]

2. GCS to Sqlite transfers
    - Non-native
        Following is an example of non-native transfers between GCS to Sqlite using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_sqlite]
               :end-before: [END transfer_non_native_gs_to_sqlite]

2. AWS S3 to Sqlite transfers
    - Non-native
        Following is an example of non-native transfers between AWS S3 to Sqlite using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_s3_to_sqlite]
               :end-before: [END transfer_non_native_s3_to_sqlite]


Transfer from Sqlite as source dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data from Sqlite the following destination dataset:

#. :ref:`table`
    - Sqlite
    - Snowflake
    - Bigquery

#. :ref:`file`
    - AWS S3
    - Google cloud storage
    - Local
    - SFTP

Following transfer modes are supported:

1. Transfer using non-native approach
2. Transfer using native approach
3. Transfer using third-party platform
