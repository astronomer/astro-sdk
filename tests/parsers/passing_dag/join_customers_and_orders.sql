---
---
SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {{agg_orders}} c LEFT OUTER JOIN {{customers_table}} p ON c.customer_id = p.customer_id