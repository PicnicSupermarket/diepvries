MERGE INTO dv.h_customer AS target
  USING (
        SELECT DISTINCT
          h_customer_hashkey,
          -- If multiple sources for the same hashkey are received, their values
          -- are concatenated using a comma.
          LISTAGG(DISTINCT r_source, ',')
              WITHIN GROUP (ORDER BY r_source)
              OVER (PARTITION BY h_customer_hashkey) AS r_source,
          r_timestamp, customer_id
        FROM dv_stg.orders_20190806_000000
        ) AS staging ON (target.h_customer_hashkey = staging.h_customer_hashkey)
  WHEN NOT MATCHED THEN INSERT (h_customer_hashkey, r_timestamp, r_source, customer_id)
    VALUES (staging.h_customer_hashkey, staging.r_timestamp, staging.r_source, staging.customer_id);
