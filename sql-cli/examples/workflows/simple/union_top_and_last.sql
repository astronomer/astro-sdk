SELECT title, rating from {{top_five_animations}}
UNION
SELECT title, rating from {{last_five_animations}};
