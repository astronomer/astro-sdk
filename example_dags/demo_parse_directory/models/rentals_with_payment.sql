---
conn_id: postgres_conn
database: pagila
---
SELECT rental.rental_id, payment.amount, rental.inventory_id FROM rental
left join payment on
    payment.rental_id = rental.rental_id