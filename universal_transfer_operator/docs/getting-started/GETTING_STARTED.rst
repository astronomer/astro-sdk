Getting Started
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Universal Transfer Operator allows data transfers between any supported source and target Datasets. It offers a consistent agnostic interface, simplifying the users' experience, so they do not need to use specific providers or operators for transfers.

This ensures a consistent set of :py:mod:`Data Providers <universal_transfer_operator.data_providers>` that can read from and write to :ref:`dataset`. The Universal Transfer
Operator can use the respective :py:mod:`Data Providers <universal_transfer_operator.data_providers>` to transfer between as a source and a destination. It also takes advantage of any existing fast and
direct high-speed endpoints, such as Snowflake’s built-in ``COPY INTO`` command to load S3 files efficiently into Snowflake.

Universal transfer operator also supports the transfers using third-party platforms like Fivetran.

.. to edit figure below refer - https://drive.google.com/file/d/1Ih0SRnMvgKTQHLJaW9k21jutjEiyacRz/view?usp=sharing
.. figure:: /images/approach.png

There are three modes to transfer data using of the ``universal_transfer_operator``.

1. :ref:`non_native`
2. :ref:`native`
3. :ref:`third_party`

More details on how transfer works can be found at :ref:`transfer_working`.
