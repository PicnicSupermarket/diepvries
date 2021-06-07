CREATE OR REPLACE TABLE dv_stg.orders_20190806_000000
  (h_customer_role_playing_hashkey TEXT (32) NOT NULL, r_timestamp TIMESTAMP_NTZ NOT NULL, r_source TEXT NOT NULL, customer_role_playing_id TEXT NOT NULL, h_order_hashkey TEXT (32) NOT NULL, order_id TEXT NOT NULL, l_order_customer_role_playing_hashkey TEXT (32) NOT NULL, ck_test_string TEXT NOT NULL, ck_test_timestamp TIMESTAMP_NTZ NOT NULL, ls_order_customer_role_playing_eff_hashdiff TEXT (32) NOT NULL, dummy_descriptive_field TEXT NOT NULL) AS
  SELECT MD5(COALESCE(customer_role_playing_id, 'dv_unknown')) AS h_customer_role_playing_hashkey, CAST('2019-08-06T00:00:00.000000Z' AS TIMESTAMP) AS r_timestamp, 'test' AS r_source, COALESCE(customer_role_playing_id, 'dv_unknown') AS customer_role_playing_id, MD5(COALESCE(order_id, 'dv_unknown')) AS h_order_hashkey, COALESCE(order_id, 'dv_unknown') AS order_id, MD5(COALESCE(order_id, 'dv_unknown')||'|~~|'||COALESCE(customer_role_playing_id, 'dv_unknown')||'|~~|'||COALESCE(CAST(ck_test_string AS VARCHAR), '')||'|~~|'||COALESCE(CAST(ck_test_timestamp AS VARCHAR), '')) AS l_order_customer_role_playing_hashkey, ck_test_string, ck_test_timestamp, MD5(REGEXP_REPLACE(COALESCE(order_id, 'dv_unknown')||'|~~|'||COALESCE(customer_role_playing_id, 'dv_unknown')||'|~~|'||COALESCE(CAST(ck_test_string AS VARCHAR), '')||'|~~|'||COALESCE(CAST(ck_test_timestamp AS VARCHAR), '')||'|~~|'||COALESCE(CAST(dummy_descriptive_field AS VARCHAR), ''), '(\\|~~\\|)+$', '')) AS ls_order_customer_role_playing_eff_hashdiff, dummy_descriptive_field
  FROM dv_extract.extract_orders;

MERGE INTO dv.h_customer AS hub
    USING (
        SELECT h_customer_role_playing_hashkey,
               MIN(r_timestamp) AS r_timestamp,MIN(r_source) AS r_source,MIN(customer_role_playing_id) AS customer_role_playing_id
        FROM dv_stg.orders_20190806_000000
        GROUP BY h_customer_role_playing_hashkey
    ) AS staging ON (hub.customer_id = staging.customer_role_playing_id)
    WHEN NOT MATCHED THEN INSERT (h_customer_hashkey, r_timestamp,r_source,customer_id)
        VALUES (staging.h_customer_role_playing_hashkey, staging.r_timestamp,staging.r_source,staging.customer_role_playing_id);

MERGE INTO dv.h_order AS hub
    USING (
        SELECT h_order_hashkey,
               MIN(r_timestamp) AS r_timestamp,MIN(r_source) AS r_source,MIN(order_id) AS order_id
        FROM dv_stg.orders_20190806_000000
        GROUP BY h_order_hashkey
    ) AS staging ON (hub.order_id = staging.order_id)
    WHEN NOT MATCHED THEN INSERT (h_order_hashkey, r_timestamp,r_source,order_id)
        VALUES (staging.h_order_hashkey, staging.r_timestamp,staging.r_source,staging.order_id);

MERGE INTO dv.l_order_customer_role_playing AS link
    USING (
        SELECT l_order_customer_role_playing_hashkey,
               MIN(h_order_hashkey) AS h_order_hashkey,MIN(h_customer_role_playing_hashkey) AS h_customer_role_playing_hashkey,MIN(order_id) AS order_id,MIN(customer_role_playing_id) AS customer_role_playing_id,MIN(ck_test_string) AS ck_test_string,MIN(ck_test_timestamp) AS ck_test_timestamp,MIN(r_timestamp) AS r_timestamp,MIN(r_source) AS r_source
        FROM dv_stg.orders_20190806_000000
        GROUP BY l_order_customer_role_playing_hashkey
    ) AS staging ON (link.l_order_customer_role_playing_hashkey = staging.l_order_customer_role_playing_hashkey)
    WHEN NOT MATCHED THEN INSERT (l_order_customer_role_playing_hashkey, h_order_hashkey,h_customer_role_playing_hashkey,order_id,customer_role_playing_id,ck_test_string,ck_test_timestamp,r_timestamp,r_source)
        VALUES (staging.l_order_customer_role_playing_hashkey, staging.h_order_hashkey,staging.h_customer_role_playing_hashkey,staging.order_id,staging.customer_role_playing_id,staging.ck_test_string,staging.ck_test_timestamp,staging.r_timestamp,staging.r_source);

MERGE INTO dv.ls_order_customer_role_playing_eff AS satellite
  USING (
    WITH
      filtered_staging AS (
        SELECT * FROM (
            SELECT
              staging.*,
              ROW_NUMBER() OVER (PARTITION BY staging.h_customer_role_playing_hashkey ORDER BY r_source, ls_order_customer_role_playing_eff_hashdiff) = 1 AS _rank
            FROM dv_stg.orders_20190806_000000 AS staging
              CROSS JOIN (
                           SELECT
                             MAX(r_timestamp) AS max_r_timestamp
                           FROM dv.ls_order_customer_role_playing_eff
                         ) AS max_satellite_timestamp
            WHERE staging.r_timestamp >=
                  COALESCE(max_satellite_timestamp.max_r_timestamp, '1970-01-01 00:00:00')
        )
        WHERE _rank=1
      ),
      effectivity_satellite AS (
        SELECT
          l.h_customer_role_playing_hashkey,
          satellite.*
        FROM filtered_staging AS staging
          INNER JOIN dv.l_order_customer_role_playing AS l
                     ON (l.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey)
          INNER JOIN dv.ls_order_customer_role_playing_eff AS satellite
                     ON (l.l_order_customer_role_playing_hashkey = satellite.l_order_customer_role_playing_hashkey
                         AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP))
      ),
      staging_satellite_affected_records AS (
        /* Records that will be inserted (don't exist in target table or exist
          in the target table but the hashdiff changed). As the r_timestamp is fetched
          from the staging table, these records will always be included in the
          WHEN NOT MATCHED condition of the MERGE command. */
        SELECT
          staging.h_customer_role_playing_hashkey,
          staging.l_order_customer_role_playing_hashkey,
          staging.ls_order_customer_role_playing_eff_hashdiff,
          staging.r_timestamp,
          staging.r_source
          ,staging.dummy_descriptive_field
        FROM filtered_staging AS staging
          LEFT JOIN effectivity_satellite AS satellite
                    ON (satellite.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey)
        WHERE satellite.l_order_customer_role_playing_hashkey IS NULL
           OR satellite.s_hashdiff <> staging.ls_order_customer_role_playing_eff_hashdiff
        UNION ALL
        /* Records from the target table that will have its r_timestamp_end updated
          (hashkey already exists in target table, but hashdiff changed). As the
          r_timestamp is fetched from the target table, these records will always be
          included in the WHEN MATCHED condition of the MERGE command. */
        SELECT
          satellite.h_customer_role_playing_hashkey,
          satellite.l_order_customer_role_playing_hashkey,
          satellite.s_hashdiff AS ls_order_customer_role_playing_eff_hashdiff,
          satellite.r_timestamp,
          satellite.r_source
          ,satellite.dummy_descriptive_field
        FROM filtered_staging AS staging
          INNER JOIN effectivity_satellite AS satellite
                     ON (satellite.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey)
        WHERE satellite.s_hashdiff <> staging.ls_order_customer_role_playing_eff_hashdiff
      )
    SELECT
      l_order_customer_role_playing_hashkey,
      ls_order_customer_role_playing_eff_hashdiff,
      r_timestamp AS r_timestamp,
      LEAD(DATEADD(milliseconds, - 1, r_timestamp), 1, CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)) OVER (PARTITION BY h_customer_role_playing_hashkey ORDER BY r_timestamp) AS r_timestamp_end,
      r_source
      ,dummy_descriptive_field
    FROM staging_satellite_affected_records
  ) AS staging
  ON (satellite.l_order_customer_role_playing_hashkey = staging.l_order_customer_role_playing_hashkey
      AND satellite.r_timestamp = staging.r_timestamp)
  WHEN MATCHED THEN
    UPDATE SET satellite.r_timestamp_end = staging.r_timestamp_end
  WHEN NOT MATCHED
    THEN
    INSERT (l_order_customer_role_playing_hashkey,s_hashdiff,r_timestamp,r_timestamp_end,r_source,dummy_descriptive_field)
      VALUES (
               staging.l_order_customer_role_playing_hashkey,
               staging.ls_order_customer_role_playing_eff_hashdiff,
               staging.r_timestamp,
               staging.r_timestamp_end,
               staging.r_source
               ,staging.dummy_descriptive_field);
