SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {aggregate_orders} c LEFT OUTER JOIN {get_customers} p ON c.customer_id = p.customer_id