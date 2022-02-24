---
conn_id: snowflake_conn
database: SANDBOX
warehouse: DEMO
schema: ASTROFLOW_CI
---
SELECT * FROM {{agg_orders}}
