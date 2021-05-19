MERGE INTO dv.l_order_customer AS link
    USING (
        SELECT l_order_customer_hashkey,
               MIN(h_order_hashkey) AS h_order_hashkey,MIN(h_customer_hashkey) AS h_customer_hashkey,MIN(order_id) AS order_id,MIN(customer_id) AS customer_id,MIN(ck_test_string) AS ck_test_string,MIN(ck_test_timestamp) AS ck_test_timestamp,MIN(r_timestamp) AS r_timestamp,MIN(r_source) AS r_source
        FROM dv_stg.orders_20190806_000000
        GROUP BY l_order_customer_hashkey
    ) AS staging ON (link.l_order_customer_hashkey = staging.l_order_customer_hashkey)
    WHEN NOT MATCHED THEN INSERT (l_order_customer_hashkey, h_order_hashkey,h_customer_hashkey,order_id,customer_id,ck_test_string,ck_test_timestamp,r_timestamp,r_source)
        VALUES (staging.l_order_customer_hashkey, staging.h_order_hashkey,staging.h_customer_hashkey,staging.order_id,staging.customer_id,staging.ck_test_string,staging.ck_test_timestamp,staging.r_timestamp,staging.r_source);
