SELECT *
FROM {{imdb_movies}}
WHERE genre1='Animation'
ORDER BY rating asc
LIMIT 5;
