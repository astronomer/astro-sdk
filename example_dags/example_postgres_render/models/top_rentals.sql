/*
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

/*
In this example, we want to find the total revenue of each store.
To show what multiple inheritance can look like in our system, we preprocess two of the joins.

Please note that if a task inherits from a task that has a conn_id/database/schema, that context will be
passed to the child query. You can also set a global context in the `aql.render` function.
*/
select
    {{store_with_inventory}}.store_id,
    sum({{rentals_with_payment}}.amount) as "total_revenue"
from {{store_with_inventory}}
left join {{rentals_with_payment}} on
    {{rentals_with_payment}}.inventory_id = {{store_with_inventory}}.inventory_id
group by
    {{store_with_inventory}}.store_id
order by
    sum({{rentals_with_payment}}.amount) desc;