---
conn_id: postgres_conn
database: pagila
---
SELECT film_id FROM film INNER JOIN film_category ON (film.film_id=film_category.film_id) WHERE film_category.category_id=6;