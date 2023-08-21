MERGE INTO dv.ls_order_customer_eff AS satellite
  USING (
        WITH
          effectivity_satellite AS (
          SELECT
            l.h_customer_hashkey,
            satellite.*
          FROM dv.l_order_customer AS l
            INNER JOIN dv.ls_order_customer_eff AS satellite
                       ON (l.l_order_customer_hashkey = satellite.l_order_customer_hashkey
                         AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP))
                                   ),
          -- Calculate minimum record timestamp for records that might be affected in the
          -- target satellite. This record timestamp will be used to filter the target satellite,
          -- ensuring the recommended cluster key (r_timestamp :: DATE) on the satellite is used,
          -- massively reducing the number of records scanned.
          min_r_timestamp AS (
          SELECT
            COALESCE(MIN(r_timestamp), CURRENT_TIMESTAMP()) AS min_r_timestamp
          FROM effectivity_satellite AS satellite
          WHERE r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)
            AND EXISTS(
                      SELECT
                        1
                      FROM dv_stg.orders_20190806_000000 AS staging
                      WHERE satellite.h_customer_hashkey = staging.h_customer_hashkey
                        AND staging.ls_order_customer_eff_hashdiff <> satellite.s_hashdiff
                      )
                             ),
          -- Filter the target satellite to include only records that might be affected
          -- during the current load. This filter will be specially effective if the target
          -- satellite is clustered using r_timestamp :: DATE.
          filtered_effectivity_satellite AS (
          SELECT *
          FROM effectivity_satellite
          WHERE r_timestamp >= (
                                              SELECT
                                                min_r_timestamp
                                              FROM min_r_timestamp
                                              )
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
          WHERE NOT EXISTS (
                           SELECT
                             1
                           FROM filtered_effectivity_satellite AS satellite
                           WHERE satellite.h_customer_hashkey = staging.h_customer_hashkey
                             AND satellite.r_timestamp >= staging.r_timestamp
                             AND satellite.s_hashdiff <> staging.ls_order_customer_eff_hashdiff
                           )
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
          LEFT JOIN filtered_effectivity_satellite AS satellite
                    ON (satellite.h_customer_hashkey = staging.h_customer_hashkey)
        WHERE satellite.l_order_customer_hashkey IS NULL
           OR satellite.s_hashdiff <> staging.ls_order_customer_eff_hashdiff
        UNION ALL
        --  Records from the target table that will have its r_timestamp_end updated
        --  (hashkey already exists in target table, but hashdiff changed). As the
        --  r_timestamp is fetched from the target table, these records will always be
        --  included in the WHEN MATCHED condition of the MERGE command.
        SELECT
          satellite.h_customer_hashkey,
          satellite.l_order_customer_hashkey,
          satellite.s_hashdiff                                AS ls_order_customer_eff_hashdiff,
          satellite.r_timestamp,
          DATEADD(NANOSECOND, -1, staging.r_timestamp) AS r_timestamp_end,
          satellite.r_source
          , satellite.dummy_descriptive_field
        FROM filtered_staging AS staging
          INNER JOIN filtered_effectivity_satellite AS satellite
                     ON (satellite.h_customer_hashkey = staging.h_customer_hashkey)
        WHERE satellite.s_hashdiff <> staging.ls_order_customer_eff_hashdiff
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
               , staging.dummy_descriptive_field);
