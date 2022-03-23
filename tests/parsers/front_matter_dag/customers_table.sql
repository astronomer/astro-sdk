---
database: foo
schema: bar
output_table:
    table_name: my_table
    schema: my_schema
---
SELECT customer_id, source, region, member_since
        FROM customers WHERE NOT is_deleted and member_since > member_date
