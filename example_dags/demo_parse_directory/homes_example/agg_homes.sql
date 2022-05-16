---
output_table:
    name: AGGREGATED_HOMES
    database: SANDBOX
---
SELECT c.BEDS, AVG(c.SELL) as AVG_SELL, AVG(c.LIST) as AVG_LIST, AVG(c.TAXES) as AVG_TAXES
FROM {{clean_homes}} c
GROUP BY c.BEDS
ORDER BY c.BEDS
