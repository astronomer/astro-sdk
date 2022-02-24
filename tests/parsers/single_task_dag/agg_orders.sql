---
conn_id: snowflake_conn
database: SANDBOX
warehouse: DEMO
schema: ASTROFLOW_CI
output_table:
    table_name: raw_table
    database: SANDBOX
    schema: ASTROFLOW_CI
---
SELECT * FROM {{input_table}}