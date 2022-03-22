SELECT *
FROM {{combine_homes}}
WHERE ROOMS > {{ params.min_rooms }}
AND ROOMS <  {{ params.max_rooms }}