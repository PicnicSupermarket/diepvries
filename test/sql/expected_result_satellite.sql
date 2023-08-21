MERGE INTO dv.hs_customer AS satellite
  USING (
        WITH
          filtered_satellite AS (
          SELECT DISTINCT
            satellite.h_customer_hashkey,
            satellite.s_hashdiff,
            satellite.r_timestamp,
            satellite.r_source
            , satellite.test_string, satellite.test_date, satellite.test_timestamp_ntz, satellite.test_integer, satellite.test_decimal, satellite.x_customer_id, satellite.grouping_key, satellite.test_geography, satellite.test_array, satellite.test_object, satellite.test_variant, satellite.test_timestamp_tz, satellite.test_timestamp_ltz, satellite.test_time, satellite.test_boolean, satellite.test_real
          FROM dv.hs_customer AS satellite
            INNER JOIN dv_stg.orders_20190806_000000 AS staging
                       ON (satellite.h_customer_hashkey = staging.h_customer_hashkey
                         AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP))
                                ),
          filtered_staging AS (
          SELECT DISTINCT
            staging.h_customer_hashkey,
            staging.hs_customer_hashdiff,
            staging.r_timestamp,
            staging.r_source
            , staging.test_string, staging.test_date, staging.test_timestamp_ntz, staging.test_integer, staging.test_decimal, staging.x_customer_id, staging.grouping_key, staging.test_geography, staging.test_array, staging.test_object, staging.test_variant, staging.test_timestamp_tz, staging.test_timestamp_ltz, staging.test_time, staging.test_boolean, staging.test_real
          FROM dv_stg.orders_20190806_000000 AS staging
            LEFT JOIN filtered_satellite AS satellite
                      ON (staging.h_customer_hashkey = satellite.h_customer_hashkey)
          WHERE satellite.h_customer_hashkey IS NULL
             OR (satellite.s_hashdiff <> staging.hs_customer_hashdiff
                AND staging.r_timestamp >= satellite.r_timestamp)
                              )
          --  Records that will be inserted (don't exist in target table or exist
          --  in the target table but the hashdiff changed). As the r_timestamp is fetched
          --  from the staging table, these records will always be included in the
          --  WHEN NOT MATCHED condition of the MERGE command.
        SELECT
          staging.h_customer_hashkey,
          staging.hs_customer_hashdiff,
          staging.r_timestamp,
          CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP) AS r_timestamp_end,
          staging.r_source
          , staging.test_string, staging.test_date, staging.test_timestamp_ntz, staging.test_integer, staging.test_decimal, staging.x_customer_id, staging.grouping_key, staging.test_geography, staging.test_array, staging.test_object, staging.test_variant, staging.test_timestamp_tz, staging.test_timestamp_ltz, staging.test_time, staging.test_boolean, staging.test_real
        FROM filtered_staging AS staging
        UNION ALL
        -- Records from the target table that will have its r_timestamp_end updated
        -- (hashkey already exists in target table, but hashdiff changed). As the
        -- r_timestamp is fetched from the target table, these records will always be
        -- included in the WHEN MATCHED condition of the MERGE command.
        SELECT
          satellite.h_customer_hashkey,
          satellite.s_hashdiff,
          satellite.r_timestamp,
          DATEADD(NANOSECONDS, - 1, staging.r_timestamp) AS r_timestamp_end,
          satellite.r_source
          , satellite.test_string, satellite.test_date, satellite.test_timestamp_ntz, satellite.test_integer, satellite.test_decimal, satellite.x_customer_id, satellite.grouping_key, satellite.test_geography, satellite.test_array, satellite.test_object, satellite.test_variant, satellite.test_timestamp_tz, satellite.test_timestamp_ltz, satellite.test_time, satellite.test_boolean, satellite.test_real
        FROM filtered_satellite AS satellite
          INNER JOIN filtered_staging AS staging
                     ON (staging.h_customer_hashkey = satellite.h_customer_hashkey)
        ) AS staging
  ON (satellite.h_customer_hashkey = staging.h_customer_hashkey
    AND satellite.r_timestamp = staging.r_timestamp)
  WHEN MATCHED THEN
    UPDATE SET satellite.r_timestamp_end = staging.r_timestamp_end
  WHEN NOT MATCHED
    THEN
    INSERT (h_customer_hashkey, s_hashdiff, r_timestamp, r_timestamp_end, r_source, test_string, test_date, test_timestamp_ntz, test_integer, test_decimal, x_customer_id, grouping_key, test_geography, test_array, test_object, test_variant, test_timestamp_tz, test_timestamp_ltz, test_time, test_boolean, test_real)
      VALUES (
               staging.h_customer_hashkey,
               staging.hs_customer_hashdiff,
               staging.r_timestamp,
               staging.r_timestamp_end,
               staging.r_source
               , staging.test_string, staging.test_date, staging.test_timestamp_ntz, staging.test_integer, staging.test_decimal, staging.x_customer_id, staging.grouping_key, staging.test_geography, staging.test_array, staging.test_object, staging.test_variant, staging.test_timestamp_tz, staging.test_timestamp_ltz, staging.test_time, staging.test_boolean, staging.test_real);
