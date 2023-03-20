# Getting Started

The Universal Transfer Operator allows data transfers between any Datasets. It copies all the data from the source Dataset to the destination Dataset. The DAG author only needs to use the Universal Transfer Operator for all transfers.

This ensures a consistent set of Data Providers that can read from and write to Datasets. The Universal Transfer
Operator can use any Data Provider as a source or a destination. It also takes advantage of any existing fast and
direct high-speed endpoints, such as Snowflake’s built-in feature to load S3 files efficiently into Snowflake.

Universal transfer operator also supports the transfers using third-party platforms like Fivetran.
