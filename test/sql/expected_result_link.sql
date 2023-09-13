-- Calculate minimum timestamp that can be affected by the current load.
-- This timestamp is used in the MERGE statement to reduce the number of records scanned, ensuring
-- the usage of the recommended clustering key (r_timestamp :: DATE).
-- If there are no matches between the staging table and the target table, the minimum timestamp is set to the
-- current timestamp minus 4 hours. The four hours are subtracted as a "safety net" to avoid the insertion of
-- duplicate records when the first version of a given hashkey is being loaded by two processes running in parallel.
-- This is unlikely to happen, but still better to play it on the safe side.
SET min_timestamp = (
                    SELECT
                      DATEADD(HOUR, -4, COALESCE(MIN(target.r_timestamp), CURRENT_TIMESTAMP()))
                    FROM dv_stg.orders_20190806_000000 AS staging
                      INNER JOIN dv.l_order_customer AS target
                                 ON (staging.l_order_customer_hashkey = target.l_order_customer_hashkey)
                    );

MERGE INTO dv.l_order_customer AS target
  USING (
        SELECT DISTINCT
          l_order_customer_hashkey,
          -- If multiple sources for the same hashkey are received, their values
          -- are concatenated using a comma.
          LISTAGG(DISTINCT r_source, ',')
                  WITHIN GROUP (ORDER BY r_source)
                  OVER (PARTITION BY l_order_customer_hashkey) AS r_source,
          h_order_hashkey, h_customer_hashkey, order_id, customer_id, ck_test_string, ck_test_timestamp, r_timestamp
        FROM dv_stg.orders_20190806_000000
        ) AS staging ON (target.l_order_customer_hashkey = staging.l_order_customer_hashkey
    AND target.r_timestamp >= $min_timestamp)
  WHEN NOT MATCHED THEN INSERT (l_order_customer_hashkey, h_order_hashkey, h_customer_hashkey, order_id, customer_id, ck_test_string, ck_test_timestamp, r_timestamp, r_source)
    VALUES (staging.l_order_customer_hashkey, staging.h_order_hashkey, staging.h_customer_hashkey, staging.order_id, staging.customer_id, staging.ck_test_string, staging.ck_test_timestamp, staging.r_timestamp, staging.r_source);
