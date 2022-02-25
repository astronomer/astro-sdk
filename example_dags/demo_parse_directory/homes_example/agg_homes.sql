---
conn_id: snowflake
database: SANDBOX
schema: KENTENDANAS
template_vars:
    clean: clean_homes
output_table:
    table_name: AGGREGATED_HOMES
    database: SANDBOX
    schema: KENTENDANAS
---
SELECT c.BEDS, AVG(c.SELL) as AVG_SELL, AVG(c.LIST) as AVG_LIST, AVG(c.TAXES) as AVG_TAXES
FROM {{clean_homes}} c
GROUP BY c.BEDS
ORDER BY c.BEDS