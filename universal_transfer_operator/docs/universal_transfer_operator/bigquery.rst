***************
Google Bigquery
***************

Transfer to Google Bigquery as destination dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data to Google Bigquery as destination as from following sources dataset:

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
    Following is an example of non-native transfers between GCS to google bigquery using non-native transfers:

    .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_gs_to_bigquery]
       :end-before: [END transfer_non_native_gs_to_bigquery]

2. Transfer using native approach
3. Transfer using third-party platform

Examples
########
1. AWS S3 to Google Bigquery transfers
    - Non-native
        Following is an example of non-native transfers between AWS S3 to Google Bigquery using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_s3_to_bigquery]
               :end-before: [END transfer_non_native_s3_to_bigquery]

2. GCS to Google Bigquery transfers
    - Non-native
        Following is an example of non-native transfers between GCS to Google Bigquery using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_bigquery]
               :end-before: [END transfer_non_native_gs_to_bigquery]

Transfer from Google Bigquery as source dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data from Google Bigquery the following destination dataset:

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
    Following is an example of non-native transfers between Bigquery to Snowflake using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_snowflake]
               :end-before: [END transfer_non_native_bigquery_to_snowflake]

2. Transfer using native approach
3. Transfer using third-party platform

Examples
########

1. Bigquery to Snowflake transfers
    - Non-native
        Following is an example of non-native transfers between Bigquery to Snowflake using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_snowflake]
               :end-before: [END transfer_non_native_bigquery_to_snowflake]

2. Bigquery to Sqlite transfers
    - Non-native
        Following is an example of non-native transfers between Bigquery to Sqlite using non-native transfers:

            .. literalinclude:: ../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_sqlite]
               :end-before: [END transfer_non_native_bigquery_to_sqlite]
