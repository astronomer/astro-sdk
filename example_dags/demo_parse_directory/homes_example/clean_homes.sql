---
conn_id: snowflake_conn
database: SANDBOX
---
SELECT *
FROM {{combine_homes}}
WHERE ROOMS > 0
AND ROOMS < 50