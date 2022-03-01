---
conn_id: snowflake_conn
database: SANDBOX
---
SELECT *
FROM {{homes}}
UNION
SELECT *
FROM {{homes2}}