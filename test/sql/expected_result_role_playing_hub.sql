-- Calculate minimum timestamp that can be affected by the current load.
-- This timestamp is used in the MERGE statement to reduce the number of records scanned, ensuring
-- the usage of the recommended clustering key (r_timestamp :: DATE).
SET min_timestamp = (
                    SELECT
                      COALESCE(MIN(target.r_timestamp), CURRENT_TIMESTAMP())
                    FROM dv_stg.orders_20190806_000000 AS staging
                      INNER JOIN dv.h_customer AS target
                                 ON (staging.h_customer_role_playing_hashkey = target.h_customer_hashkey)
                    );

MERGE INTO dv.h_customer AS target
  USING (
        SELECT DISTINCT
          h_customer_role_playing_hashkey,
          -- If multiple sources for the same hashkey are received, their values
          -- are concatenated using a comma.
          LISTAGG(DISTINCT r_source, ',')
                  WITHIN GROUP (ORDER BY r_source)
                  OVER (PARTITION BY h_customer_role_playing_hashkey) AS r_source,
          r_timestamp, customer_role_playing_id
        FROM dv_stg.orders_20190806_000000
        ) AS staging ON (target.h_customer_hashkey = staging.h_customer_role_playing_hashkey
    AND target.r_timestamp >= $min_timestamp)
  WHEN NOT MATCHED THEN INSERT (h_customer_hashkey, r_timestamp, r_source, customer_id)
    VALUES (staging.h_customer_role_playing_hashkey, staging.r_timestamp, staging.r_source, staging.customer_role_playing_id);
