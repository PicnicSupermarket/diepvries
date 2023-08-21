MERGE INTO dv.ls_order_customer_eff AS satellite
  USING (
        WITH
          filtered_effectivity_satellite AS (
          SELECT DISTINCT
            l.h_customer_hashkey,
            satellite.*
          FROM dv.l_order_customer AS l
            INNER JOIN dv.ls_order_customer_eff AS satellite
                       ON (l.l_order_customer_hashkey = satellite.l_order_customer_hashkey
                         AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP))
            INNER JOIN dv_stg.orders_20190806_000000 AS staging
                       ON (l.h_customer_hashkey = staging.h_customer_hashkey)
                                            ),
          filtered_staging AS (
          SELECT DISTINCT
            staging.h_customer_hashkey,
            staging.l_order_customer_hashkey,
            staging.ls_order_customer_eff_hashdiff,
            staging.r_timestamp,
            staging.r_source
            , staging.dummy_descriptive_field
          FROM dv_stg.orders_20190806_000000 AS staging
            LEFT JOIN filtered_effectivity_satellite AS satellite
                      ON (satellite.h_customer_hashkey = staging.h_customer_hashkey)
          WHERE satellite.l_order_customer_hashkey IS NULL
             OR (staging.ls_order_customer_eff_hashdiff <> satellite.s_hashdiff
                AND staging.r_timestamp >= satellite.r_timestamp)
                              )
        --   Records that will be inserted (don't exist in target table or exist
        --   in the target table but the hashdiff changed). As the r_timestamp is fetched
        --   from the staging table, these records will always be included in the
        --   WHEN NOT MATCHED condition of the MERGE command.
        SELECT
          staging.h_customer_hashkey,
          staging.l_order_customer_hashkey,
          staging.ls_order_customer_eff_hashdiff,
          staging.r_timestamp,
          CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP) AS r_timestamp_end,
          staging.r_source
          , staging.dummy_descriptive_field
        FROM filtered_staging AS staging
        UNION ALL
        --  Records from the target table that will have its r_timestamp_end updated
        --  (hashkey already exists in target table, but hashdiff changed). As the
        --  r_timestamp is fetched from the target table, these records will always be
        --  included in the WHEN MATCHED condition of the MERGE command.
        SELECT
          satellite.h_customer_hashkey,
          satellite.l_order_customer_hashkey,
          satellite.s_hashdiff AS ls_order_customer_eff_hashdiff,
          satellite.r_timestamp,
          DATEADD(NANOSECONDS, - 1, staging.r_timestamp) AS r_timestamp_end,
          satellite.r_source
          , satellite.dummy_descriptive_field
        FROM filtered_effectivity_satellite AS satellite
          INNER JOIN filtered_staging AS staging
                     ON (satellite.h_customer_hashkey = staging.h_customer_hashkey)
        ) AS staging
  ON (satellite.l_order_customer_hashkey = staging.l_order_customer_hashkey
    AND satellite.r_timestamp = staging.r_timestamp)
  WHEN MATCHED THEN
    UPDATE SET satellite.r_timestamp_end = staging.r_timestamp_end
  WHEN NOT MATCHED
    THEN
    INSERT (l_order_customer_hashkey, s_hashdiff, r_timestamp, r_timestamp_end, r_source, dummy_descriptive_field)
      VALUES (
               staging.l_order_customer_hashkey,
               staging.ls_order_customer_eff_hashdiff,
               staging.r_timestamp,
               staging.r_timestamp_end,
               staging.r_source
               , dummy_descriptive_field);
