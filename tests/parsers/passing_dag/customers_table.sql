SELECT customer_id, source, region, member_since
        FROM {get_new_customers} WHERE NOT is_deleted