SELECT TOP 5 title, rating
FROM {{ input_table }}
WHERE genre1='Animation'
ORDER BY rating desc
