MERGE INTO dv.l_order_customer AS target
  USING (
        SELECT DISTINCT
          l_order_customer_hashkey,h_order_hashkey,h_customer_hashkey,order_id,customer_id,ck_test_string,ck_test_timestamp,r_timestamp,r_source
        FROM dv_stg.orders_20190806_000000
        ) AS staging ON (target.l_order_customer_hashkey = staging.l_order_customer_hashkey)
  WHEN NOT MATCHED THEN INSERT (l_order_customer_hashkey,h_order_hashkey,h_customer_hashkey,order_id,customer_id,ck_test_string,ck_test_timestamp,r_timestamp,r_source)
    VALUES (staging.l_order_customer_hashkey,staging.h_order_hashkey,staging.h_customer_hashkey,staging.order_id,staging.customer_id,staging.ck_test_string,staging.ck_test_timestamp,staging.r_timestamp,staging.r_source);
