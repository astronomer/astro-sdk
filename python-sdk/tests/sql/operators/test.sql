SELECT {{actor}}.actor_id, first_name, last_name, COUNT(film_id)
FROM {{actor}} JOIN {{film_actor_join}} ON {{actor}}.actor_id = {{film_actor_join}}.actor_id
WHERE last_name LIKE {{unsafe_parameter}} GROUP BY {{actor}}.actor_id
