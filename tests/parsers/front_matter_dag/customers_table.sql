---
database: foo
schema: bar
template_vars:
    customers: get_customers
---
SELECT customer_id, source, region, member_since
        FROM customers WHERE NOT is_deleted and member_since > member_date