---
conn_id: sqlite_conn
---
SELECT Title, Rating
FROM movies
WHERE Genre1=='Animation'
ORDER BY Rating desc
LIMIT 5;
