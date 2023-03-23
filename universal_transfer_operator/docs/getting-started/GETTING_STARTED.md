# Getting Started

The Universal Transfer Operator allows data transfers between any supported source and target Datasets. It offers a consistent agnostic interface, simplifying the users' experience, so they do not need to use specific providers or operators for transfers.

This ensures a consistent set of Data Providers that can read from and write to Datasets. The Universal Transfer
Operator can use any Data Provider as a source or a destination. It also takes advantage of any existing fast and
direct high-speed endpoints, such as Snowflake’s built-in `COPY INTO` command to load S3 files efficiently into Snowflake.

Universal transfer operator also supports the transfers using third-party platforms like Fivetran.
