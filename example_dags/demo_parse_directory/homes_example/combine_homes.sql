---
conn_id: snowflake
database: SANDBOX
schema: KENTENDANAS
output_table:
    table_name: COMBINED_HOMES
    database: SANDBOX
    schema: KENTENDANAS
---
SELECT *
FROM HOMES
UNION
SELECT *
FROM HOMES2