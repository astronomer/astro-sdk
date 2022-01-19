SELECT customer_id, count(*) AS purchase_count FROM orders_table
        WHERE purchase_date >= DATEADD(day, -7, '{{ execution_date }}')