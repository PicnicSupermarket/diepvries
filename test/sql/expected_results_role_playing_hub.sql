MERGE INTO dv.h_customer AS hub
    USING (
        SELECT h_customer_role_playing_hashkey,
               MIN(r_timestamp) AS r_timestamp,MIN(r_source) AS r_source,MIN(customer_role_playing_id) AS customer_role_playing_id
        FROM dv_stg.orders_20190806_000000
        GROUP BY h_customer_role_playing_hashkey
    ) AS staging ON (hub.customer_id = staging.customer_role_playing_id)
    WHEN NOT MATCHED THEN INSERT (h_customer_hashkey, r_timestamp,r_source,customer_id)
        VALUES (staging.h_customer_role_playing_hashkey, staging.r_timestamp,staging.r_source,staging.customer_role_playing_id);
