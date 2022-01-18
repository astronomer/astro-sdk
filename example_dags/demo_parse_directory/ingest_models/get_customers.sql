---
database: my_db
schema: my_schema
---
SELECT customer_id, source, region, member_since
        FROM customers WHERE NOT is_deleted