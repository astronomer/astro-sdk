***************
Sqlite
***************

Transfer to Sqlite as destination dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data to Sqlite as destination as from following sources dataset:

#. :ref:`table`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. :ref:`non_native`
    Following is an example of non-native transfers between google bigquery to Sqlite using non-native transfer:

    .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
       :language: python
       :start-after: [START transfer_non_native_bigquery_to_sqlite]
       :end-before: [END transfer_non_native_bigquery_to_sqlite]

2. :ref:`third_party`

Examples
########
1. Google Bigquery to Sqlite transfers
    - :ref:`non_native`
        Following is an example of non-native transfers between Google Bigquery to Sqlite using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_bigquery_to_sqlite]
               :end-before: [END transfer_non_native_bigquery_to_sqlite]

2. GCS to Sqlite transfers
    - :ref:`non_native`
        Following is an example of non-native transfers between GCS to Sqlite using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_gs_to_sqlite]
               :end-before: [END transfer_non_native_gs_to_sqlite]

2. AWS S3 to Sqlite transfers
    - :ref:`non_native`
        Following is an example of non-native transfers between AWS S3 to Sqlite using non-native transfer:

            .. literalinclude:: ../../../../example_dags/example_universal_transfer_operator.py
               :language: python
               :start-after: [START transfer_non_native_s3_to_sqlite]
               :end-before: [END transfer_non_native_s3_to_sqlite]


Transfer from Sqlite as source dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
User can transfer data from Sqlite to the following destination dataset:

#. :ref:`table`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START database]
       :end-before: [END database]

#. :ref:`file`

    .. literalinclude:: ../../../../src/universal_transfer_operator/constants.py
       :language: python
       :start-after: [START filelocation]
       :end-before: [END filelocation]

Following transfer modes are supported:

1. :ref:`non_native`
2. :ref:`third_party`
