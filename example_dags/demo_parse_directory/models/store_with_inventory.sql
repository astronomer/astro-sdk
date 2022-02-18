---
conn_id: postgres_conn
database: pagila
---
SELECT store.store_id, inventory.inventory_id from
    store
left join inventory on
    inventory.store_id = store.store_id