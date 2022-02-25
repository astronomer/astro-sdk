---
conn_id: snowflake
database: SANDBOX
schema: KENTENDANAS
template_vars:
    combine: combine_homes
output_table:
    table_name: CLEANED_HOMES
    database: SANDBOX
    schema: KENTENDANAS
---
SELECT *
FROM {{combine_homes}}
WHERE ROOMS > 0
AND ROOMS < 50