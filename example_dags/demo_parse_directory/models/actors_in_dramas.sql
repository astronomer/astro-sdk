---
template_vars:
    drama_films: drama_films
    actor: actors_c_lastname
---
SELECT DISTINCT first_name, last_name FROM actor INNER JOIN drama_films ON (actor.actor_id=tmp_astro.drama_actors.actor_id);