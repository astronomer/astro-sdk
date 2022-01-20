---
template_vars:
    customers: load_from_s3
---
SELECT customer_id, source, region, member_since
        FROM customers WHERE NOT is_deleted and member_since > member_date