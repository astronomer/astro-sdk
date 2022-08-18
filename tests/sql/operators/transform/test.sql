SELECT Title, Rating
FROM {{ input_table }}
WHERE Genre1=='Animation'
ORDER BY Rating desc
LIMIT 5;
