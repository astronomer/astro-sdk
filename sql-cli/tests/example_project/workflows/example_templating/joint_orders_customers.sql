SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type
FROM {{ filtered_orders }} f
JOIN customers c
ON f.customer_id = c.customer_id
