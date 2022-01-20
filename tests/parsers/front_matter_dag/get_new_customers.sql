---
template_vars:
    customers: customers_table
output_table:
    database: my_db
    conn_id: my_conn_id
---
SELECT * FROM customers WHERE member_since > DATEADD(day, -7, '{{ execution_date }}')