SELECT title, rating from {{source__top_five_animations}}
UNION
SELECT title, rating from {{source__last_five_animations}};
