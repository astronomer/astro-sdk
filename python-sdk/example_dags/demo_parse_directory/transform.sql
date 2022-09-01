SELECT title, rating
FROM {{ input_table }}
WHERE genre1=='Animation'
ORDER BY rating desc
LIMIT 5;
