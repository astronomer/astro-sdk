---
conn_id: snowflake_conn
---
SELECT *
FROM {{ orders }}
LIMIT 5;
