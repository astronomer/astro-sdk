---
conn_id: snowflake_conn
database: DWH_LEGACY
warehouse: TRANSFORMING
template_vars:
    foo: agg_orders
---
SELECT * FROM foo