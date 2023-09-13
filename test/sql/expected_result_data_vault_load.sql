CREATE OR REPLACE TABLE dv_stg.orders_20190806_000000
  (h_customer_hashkey TEXT (32) NOT NULL, r_timestamp TIMESTAMP_NTZ NOT NULL, r_source TEXT NOT NULL, customer_id TEXT NOT NULL, h_customer_role_playing_hashkey TEXT (32) NOT NULL, customer_role_playing_id TEXT NOT NULL, h_order_hashkey TEXT (32) NOT NULL, order_id TEXT NOT NULL, l_order_customer_hashkey TEXT (32) NOT NULL, ck_test_string TEXT NOT NULL, ck_test_timestamp TIMESTAMP_NTZ NOT NULL, l_order_customer_role_playing_hashkey TEXT (32) NOT NULL, hs_customer_hashdiff TEXT (32) NOT NULL, test_string TEXT, test_date DATE, test_timestamp_ntz TIMESTAMP_NTZ, test_integer NUMBER (38, 0), test_decimal NUMBER (18, 8), x_customer_id TEXT, grouping_key TEXT, test_geography GEOGRAPHY, test_array ARRAY, test_object OBJECT, test_variant VARIANT, test_timestamp_tz TIMESTAMP_TZ, test_timestamp_ltz TIMESTAMP_LTZ, test_time TIME, test_boolean BOOLEAN, test_real REAL, ls_order_customer_eff_hashdiff TEXT (32) NOT NULL, dummy_descriptive_field TEXT NOT NULL, ls_order_customer_role_playing_eff_hashdiff TEXT (32) NOT NULL) AS
  SELECT MD5(COALESCE(CAST(customer_id AS TEXT), 'dv_unknown')) AS h_customer_hashkey, CAST('2019-08-06T00:00:00.000000Z' AS TIMESTAMP) AS r_timestamp, 'test' AS r_source, COALESCE(customer_id, 'dv_unknown') AS customer_id, MD5(COALESCE(CAST(customer_role_playing_id AS TEXT), 'dv_unknown')) AS h_customer_role_playing_hashkey, COALESCE(customer_role_playing_id, 'dv_unknown') AS customer_role_playing_id, MD5(COALESCE(CAST(order_id AS TEXT), 'dv_unknown')) AS h_order_hashkey, COALESCE(order_id, 'dv_unknown') AS order_id, MD5(COALESCE(CAST(order_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(customer_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(ck_test_string AS TEXT), '')||'|~~|'||COALESCE(TO_CHAR(CAST(ck_test_timestamp AS TIMESTAMP_NTZ), 'yyyy-mm-dd hh24:mi:ss.ff9'), '')) AS l_order_customer_hashkey, ck_test_string, ck_test_timestamp, MD5(COALESCE(CAST(order_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(customer_role_playing_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(ck_test_string AS TEXT), '')||'|~~|'||COALESCE(TO_CHAR(CAST(ck_test_timestamp AS TIMESTAMP_NTZ), 'yyyy-mm-dd hh24:mi:ss.ff9'), '')) AS l_order_customer_role_playing_hashkey, MD5(REGEXP_REPLACE(COALESCE(CAST(customer_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(test_string AS TEXT), '')||'|~~|'||COALESCE(TO_CHAR(CAST(test_date AS DATE), 'yyyy-mm-dd'), '')||'|~~|'||COALESCE(TO_CHAR(CAST(test_timestamp_ntz AS TIMESTAMP_NTZ), 'yyyy-mm-dd hh24:mi:ss.ff9'), '')||'|~~|'||COALESCE(CAST(CAST(test_integer AS NUMBER (38, 0)) AS TEXT), '')||'|~~|'||COALESCE(CAST(CAST(test_decimal AS NUMBER (18, 8)) AS TEXT), '')||'|~~|'||COALESCE(CAST(x_customer_id AS TEXT), '')||'|~~|'||COALESCE(CAST(grouping_key AS TEXT), '')||'|~~|'||COALESCE(ST_ASTEXT(TO_GEOGRAPHY(test_geography)), '')||'|~~|'||COALESCE(CAST(CAST(test_array AS ARRAY) AS TEXT), '')||'|~~|'||COALESCE(CAST(CAST(test_object AS OBJECT) AS TEXT), '')||'|~~|'||COALESCE(CAST(CAST(test_variant AS VARIANT) AS TEXT), '')||'|~~|'||COALESCE(TO_CHAR(CAST(test_timestamp_tz AS TIMESTAMP_TZ), 'yyyy-mm-dd hh24:mi:ss.ff9 tzhtzm'), '')||'|~~|'||COALESCE(TO_CHAR(CAST(test_timestamp_ltz AS TIMESTAMP_LTZ), 'yyyy-mm-dd hh24:mi:ss.ff9 tzhtzm'), '')||'|~~|'||COALESCE(TO_CHAR(CAST(test_time AS TIME), 'hh24:mi:ss.ff9'), '')||'|~~|'||COALESCE(CAST(CAST(test_boolean AS BOOLEAN) AS TEXT), '')||'|~~|'||COALESCE(CAST(CAST(test_real AS REAL) AS TEXT), ''), '(\\|~~\\|)+$', '')) AS hs_customer_hashdiff, test_string, test_date, test_timestamp_ntz, test_integer, test_decimal, x_customer_id, grouping_key, test_geography, test_array, test_object, test_variant, test_timestamp_tz, test_timestamp_ltz, test_time, test_boolean, test_real, MD5(REGEXP_REPLACE(COALESCE(CAST(order_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(customer_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(ck_test_string AS TEXT), '')||'|~~|'||COALESCE(TO_CHAR(CAST(ck_test_timestamp AS TIMESTAMP_NTZ), 'yyyy-mm-dd hh24:mi:ss.ff9'), '')||'|~~|'||COALESCE(CAST(dummy_descriptive_field AS TEXT), ''), '(\\|~~\\|)+$', '')) AS ls_order_customer_eff_hashdiff, dummy_descriptive_field, MD5(REGEXP_REPLACE(COALESCE(CAST(order_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(customer_role_playing_id AS TEXT), 'dv_unknown')||'|~~|'||COALESCE(CAST(ck_test_string AS TEXT), '')||'|~~|'||COALESCE(TO_CHAR(CAST(ck_test_timestamp AS TIMESTAMP_NTZ), 'yyyy-mm-dd hh24:mi:ss.ff9'), '')||'|~~|'||COALESCE(CAST(dummy_descriptive_field AS TEXT), ''), '(\\|~~\\|)+$', '')) AS ls_order_customer_role_playing_eff_hashdiff
  FROM dv_extract.extract_orders;

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
                      INNER JOIN dv.h_customer AS target
                                 ON (staging.h_customer_hashkey = target.h_customer_hashkey)
                    );

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
        ) AS staging ON (target.h_customer_hashkey = staging.h_customer_hashkey
    AND target.r_timestamp >= $min_timestamp)
  WHEN NOT MATCHED THEN INSERT (h_customer_hashkey, r_timestamp, r_source, customer_id)
    VALUES (staging.h_customer_hashkey, staging.r_timestamp, staging.r_source, staging.customer_id);

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
                      INNER JOIN dv.h_order AS target
                                 ON (staging.h_order_hashkey = target.h_order_hashkey)
                    );

MERGE INTO dv.h_order AS target
  USING (
        SELECT DISTINCT
          h_order_hashkey,
          -- If multiple sources for the same hashkey are received, their values
          -- are concatenated using a comma.
          LISTAGG(DISTINCT r_source, ',')
                  WITHIN GROUP (ORDER BY r_source)
                  OVER (PARTITION BY h_order_hashkey) AS r_source,
          r_timestamp, order_id
        FROM dv_stg.orders_20190806_000000
        ) AS staging ON (target.h_order_hashkey = staging.h_order_hashkey
    AND target.r_timestamp >= $min_timestamp)
  WHEN NOT MATCHED THEN INSERT (h_order_hashkey, r_timestamp, r_source, order_id)
    VALUES (staging.h_order_hashkey, staging.r_timestamp, staging.r_source, staging.order_id);

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
                      INNER JOIN dv.l_order_customer_role_playing AS target
                                 ON (staging.l_order_customer_role_playing_hashkey = target.l_order_customer_role_playing_hashkey)
                    );

MERGE INTO dv.l_order_customer_role_playing AS target
  USING (
        SELECT DISTINCT
          l_order_customer_role_playing_hashkey,
          -- If multiple sources for the same hashkey are received, their values
          -- are concatenated using a comma.
          LISTAGG(DISTINCT r_source, ',')
                  WITHIN GROUP (ORDER BY r_source)
                  OVER (PARTITION BY l_order_customer_role_playing_hashkey) AS r_source,
          h_order_hashkey, h_customer_role_playing_hashkey, order_id, customer_role_playing_id, ck_test_string, ck_test_timestamp, r_timestamp
        FROM dv_stg.orders_20190806_000000
        ) AS staging ON (target.l_order_customer_role_playing_hashkey = staging.l_order_customer_role_playing_hashkey
    AND target.r_timestamp >= $min_timestamp)
  WHEN NOT MATCHED THEN INSERT (l_order_customer_role_playing_hashkey, h_order_hashkey, h_customer_role_playing_hashkey, order_id, customer_role_playing_id, ck_test_string, ck_test_timestamp, r_timestamp, r_source)
    VALUES (staging.l_order_customer_role_playing_hashkey, staging.h_order_hashkey, staging.h_customer_role_playing_hashkey, staging.order_id, staging.customer_role_playing_id, staging.ck_test_string, staging.ck_test_timestamp, staging.r_timestamp, staging.r_source);

-- Calculate minimum timestamp that can be affected by the current load.
-- This timestamp is used in the MERGE statement to reduce the number of records scanned, ensuring
-- the usage of the recommended clustering key (r_timestamp :: DATE).
-- If there are no matches between the staging table and the target table, the minimum timestamp is set to the
-- current timestamp minus 4 hours. The four hours are subtracted as a "safety net" to avoid the insertion of
-- duplicate records when the first version of a given hashkey is being loaded by two processes running in parallel.
-- This is unlikely to happen, but still better to play it on the safe side.
SET min_timestamp = (
                    SELECT
                      DATEADD(HOUR, -4, COALESCE(MIN(satellite.r_timestamp), CURRENT_TIMESTAMP()))
                    FROM dv_stg.orders_20190806_000000 AS staging
                      INNER JOIN dv.hs_customer AS satellite
                                 ON (satellite.h_customer_hashkey = staging.h_customer_hashkey
                                   AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP))
                    );

MERGE INTO dv.hs_customer AS satellite
  USING (
        WITH
          filtered_satellite AS (
          SELECT *
          FROM dv.hs_customer
          WHERE r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)
            AND r_timestamp >= $min_timestamp
                                ),
          filtered_staging AS (
          SELECT DISTINCT
            staging.h_customer_hashkey,
            staging.hs_customer_hashdiff,
            staging.r_timestamp,
            staging.r_source
            , staging.test_string, staging.test_date, staging.test_timestamp_ntz, staging.test_integer, staging.test_decimal, staging.x_customer_id, staging.grouping_key, staging.test_geography, staging.test_array, staging.test_object, staging.test_variant, staging.test_timestamp_tz, staging.test_timestamp_ltz, staging.test_time, staging.test_boolean, staging.test_real
          FROM dv_stg.orders_20190806_000000 AS staging
          WHERE NOT EXISTS (
                           SELECT
                             1
                           FROM filtered_satellite AS satellite
                           WHERE staging.h_customer_hashkey = satellite.h_customer_hashkey
                             AND satellite.r_timestamp >= staging.r_timestamp
                           )
                              ),
          --  Records that will be inserted (don't exist in target table or exist
          --  in the target table but the hashdiff changed). As the r_timestamp is fetched
          --  from the staging table, these records will always be included in the
          --  WHEN NOT MATCHED condition of the MERGE command.
          staging_satellite_affected_records AS (
          SELECT
            staging.h_customer_hashkey,
            staging.hs_customer_hashdiff,
            staging.r_timestamp,
            staging.r_source
            , staging.test_string, staging.test_date, staging.test_timestamp_ntz, staging.test_integer, staging.test_decimal, staging.x_customer_id, staging.grouping_key, staging.test_geography, staging.test_array, staging.test_object, staging.test_variant, staging.test_timestamp_tz, staging.test_timestamp_ltz, staging.test_time, staging.test_boolean, staging.test_real
          FROM filtered_staging AS staging
            LEFT OUTER JOIN filtered_satellite AS satellite
                            ON (staging.h_customer_hashkey = satellite.h_customer_hashkey
                              AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP))
          WHERE satellite.h_customer_hashkey IS NULL
             OR satellite.s_hashdiff <> staging.hs_customer_hashdiff
          UNION ALL
          -- Records from the target table that will have its r_timestamp_end updated
          -- (hashkey already exists in target table, but hashdiff changed). As the
          -- r_timestamp is fetched from the target table, these records will always be
          -- included in the WHEN MATCHED condition of the MERGE command.
          SELECT
            satellite.h_customer_hashkey,
            satellite.s_hashdiff,
            satellite.r_timestamp,
            satellite.r_source
            , satellite.test_string, satellite.test_date, satellite.test_timestamp_ntz, satellite.test_integer, satellite.test_decimal, satellite.x_customer_id, satellite.grouping_key, satellite.test_geography, satellite.test_array, satellite.test_object, satellite.test_variant, satellite.test_timestamp_tz, satellite.test_timestamp_ltz, satellite.test_time, satellite.test_boolean, satellite.test_real
          FROM filtered_satellite AS satellite
            INNER JOIN filtered_staging AS staging
                       ON (staging.h_customer_hashkey = satellite.h_customer_hashkey
                         AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP))
          WHERE staging.hs_customer_hashdiff <> satellite.s_hashdiff
                                                )
        SELECT
          h_customer_hashkey,
          hs_customer_hashdiff,
          r_timestamp AS r_timestamp,
          LEAD(DATEADD(milliseconds, - 1, r_timestamp), 1, CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)) OVER (PARTITION BY h_customer_hashkey ORDER BY r_timestamp) AS r_timestamp_end,
          r_source
          , test_string, test_date, test_timestamp_ntz, test_integer, test_decimal, x_customer_id, grouping_key, test_geography, test_array, test_object, test_variant, test_timestamp_tz, test_timestamp_ltz, test_time, test_boolean, test_real
        FROM staging_satellite_affected_records
        ) AS staging
  ON (satellite.h_customer_hashkey = staging.h_customer_hashkey
    AND satellite.r_timestamp = staging.r_timestamp
    AND satellite.r_timestamp >= $min_timestamp)
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

-- Calculate minimum timestamp that can be affected by the current load.
-- This timestamp is used in the MERGE statement to reduce the number of records scanned, ensuring
-- the usage of the recommended clustering key (r_timestamp :: DATE).
-- If there are no matches between the staging table and the target table, the minimum timestamp is set to the
-- current timestamp minus 4 hours. The four hours are subtracted as a "safety net" to avoid the insertion of
-- duplicate records when the first version of a given driving key is being loaded by two processes running in parallel.
-- This is unlikely to happen, but still better to play it on the safe side.
SET min_timestamp_link = (
                         SELECT
                           DATEADD(HOUR, -4, COALESCE(MIN(l.r_timestamp), CURRENT_TIMESTAMP()))
                         FROM dv.l_order_customer AS l
                           INNER JOIN dv_stg.orders_20190806_000000 AS staging
                                      ON (l.h_customer_hashkey = staging.h_customer_hashkey)
                         );

SET min_timestamp_satellite = (
                              SELECT
                                DATEADD(HOUR, -4, COALESCE(MIN(satellite.r_timestamp), CURRENT_TIMESTAMP()))
                              FROM dv.l_order_customer AS l
                                INNER JOIN dv.ls_order_customer_eff AS satellite
                                           ON (l.l_order_customer_hashkey = satellite.l_order_customer_hashkey
                                             AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)
                                             AND l.r_timestamp >= $min_timestamp_link)
                                INNER JOIN dv_stg.orders_20190806_000000 AS staging
                                           ON (l.h_customer_hashkey = staging.h_customer_hashkey)
                              );

MERGE INTO dv.ls_order_customer_eff AS satellite
  USING (
        WITH
          filtered_effectivity_satellite AS (
          SELECT
            l.h_customer_hashkey,
            satellite.*
          FROM dv_stg.orders_20190806_000000 AS staging
            INNER JOIN dv.l_order_customer AS l
                       ON (l.h_customer_hashkey = staging.h_customer_hashkey
                         AND l.r_timestamp >= $min_timestamp_link)
            INNER JOIN dv.ls_order_customer_eff AS satellite
                       ON (l.l_order_customer_hashkey = satellite.l_order_customer_hashkey
                         AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)
                         AND satellite.r_timestamp >= $min_timestamp_satellite)
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
                           )
                              ),
          --   Records that will be inserted (don't exist in target table or exist
          --   in the target table but the hashdiff changed). As the r_timestamp is fetched
          --   from the staging table, these records will always be included in the
          --   WHEN NOT MATCHED condition of the MERGE command.
          staging_satellite_affected_records AS (
          SELECT
            staging.h_customer_hashkey,
            staging.l_order_customer_hashkey,
            staging.ls_order_customer_eff_hashdiff,
            staging.r_timestamp,
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
            satellite.s_hashdiff AS ls_order_customer_eff_hashdiff,
            satellite.r_timestamp,
            satellite.r_source
            , satellite.dummy_descriptive_field
          FROM filtered_staging AS staging
            INNER JOIN filtered_effectivity_satellite AS satellite
                       ON (satellite.h_customer_hashkey = staging.h_customer_hashkey)
          WHERE satellite.s_hashdiff <> staging.ls_order_customer_eff_hashdiff
                                                )
        SELECT
          l_order_customer_hashkey,
          ls_order_customer_eff_hashdiff,
          r_timestamp AS r_timestamp,
          LEAD(DATEADD(milliseconds, - 1, r_timestamp), 1, CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)) OVER (PARTITION BY h_customer_hashkey ORDER BY r_timestamp) AS r_timestamp_end,
          r_source
          , dummy_descriptive_field
        FROM staging_satellite_affected_records
        ) AS staging
  ON (satellite.l_order_customer_hashkey = staging.l_order_customer_hashkey
    AND satellite.r_timestamp = staging.r_timestamp
    AND satellite.r_timestamp >= $min_timestamp_satellite)
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

-- Calculate minimum timestamp that can be affected by the current load.
-- This timestamp is used in the MERGE statement to reduce the number of records scanned, ensuring
-- the usage of the recommended clustering key (r_timestamp :: DATE).
-- If there are no matches between the staging table and the target table, the minimum timestamp is set to the
-- current timestamp minus 4 hours. The four hours are subtracted as a "safety net" to avoid the insertion of
-- duplicate records when the first version of a given driving key is being loaded by two processes running in parallel.
-- This is unlikely to happen, but still better to play it on the safe side.
SET min_timestamp_link = (
                         SELECT
                           DATEADD(HOUR, -4, COALESCE(MIN(l.r_timestamp), CURRENT_TIMESTAMP()))
                         FROM dv.l_order_customer_role_playing AS l
                           INNER JOIN dv_stg.orders_20190806_000000 AS staging
                                      ON (l.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey)
                         );

SET min_timestamp_satellite = (
                              SELECT
                                DATEADD(HOUR, -4, COALESCE(MIN(satellite.r_timestamp), CURRENT_TIMESTAMP()))
                              FROM dv.l_order_customer_role_playing AS l
                                INNER JOIN dv.ls_order_customer_role_playing_eff AS satellite
                                           ON (l.l_order_customer_role_playing_hashkey = satellite.l_order_customer_role_playing_hashkey
                                             AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)
                                             AND l.r_timestamp >= $min_timestamp_link)
                                INNER JOIN dv_stg.orders_20190806_000000 AS staging
                                           ON (l.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey)
                              );

MERGE INTO dv.ls_order_customer_role_playing_eff AS satellite
  USING (
        WITH
          filtered_effectivity_satellite AS (
          SELECT
            l.h_customer_role_playing_hashkey,
            satellite.*
          FROM dv_stg.orders_20190806_000000 AS staging
            INNER JOIN dv.l_order_customer_role_playing AS l
                       ON (l.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey
                         AND l.r_timestamp >= $min_timestamp_link)
            INNER JOIN dv.ls_order_customer_role_playing_eff AS satellite
                       ON (l.l_order_customer_role_playing_hashkey = satellite.l_order_customer_role_playing_hashkey
                         AND satellite.r_timestamp_end = CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)
                         AND satellite.r_timestamp >= $min_timestamp_satellite)
                                            ),
          filtered_staging AS (
          SELECT DISTINCT
            staging.h_customer_role_playing_hashkey,
            staging.l_order_customer_role_playing_hashkey,
            staging.ls_order_customer_role_playing_eff_hashdiff,
            staging.r_timestamp,
            staging.r_source
            , staging.dummy_descriptive_field
          FROM dv_stg.orders_20190806_000000 AS staging
          WHERE NOT EXISTS (
                           SELECT
                             1
                           FROM filtered_effectivity_satellite AS satellite
                           WHERE satellite.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey
                             AND satellite.r_timestamp >= staging.r_timestamp
                           )
                              ),
          --   Records that will be inserted (don't exist in target table or exist
          --   in the target table but the hashdiff changed). As the r_timestamp is fetched
          --   from the staging table, these records will always be included in the
          --   WHEN NOT MATCHED condition of the MERGE command.
          staging_satellite_affected_records AS (
          SELECT
            staging.h_customer_role_playing_hashkey,
            staging.l_order_customer_role_playing_hashkey,
            staging.ls_order_customer_role_playing_eff_hashdiff,
            staging.r_timestamp,
            staging.r_source
            , staging.dummy_descriptive_field
          FROM filtered_staging AS staging
            LEFT JOIN filtered_effectivity_satellite AS satellite
                      ON (satellite.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey)
          WHERE satellite.l_order_customer_role_playing_hashkey IS NULL
             OR satellite.s_hashdiff <> staging.ls_order_customer_role_playing_eff_hashdiff
          UNION ALL
          --  Records from the target table that will have its r_timestamp_end updated
          --  (hashkey already exists in target table, but hashdiff changed). As the
          --  r_timestamp is fetched from the target table, these records will always be
          --  included in the WHEN MATCHED condition of the MERGE command.
          SELECT
            satellite.h_customer_role_playing_hashkey,
            satellite.l_order_customer_role_playing_hashkey,
            satellite.s_hashdiff AS ls_order_customer_role_playing_eff_hashdiff,
            satellite.r_timestamp,
            satellite.r_source
            , satellite.dummy_descriptive_field
          FROM filtered_staging AS staging
            INNER JOIN filtered_effectivity_satellite AS satellite
                       ON (satellite.h_customer_role_playing_hashkey = staging.h_customer_role_playing_hashkey)
          WHERE satellite.s_hashdiff <> staging.ls_order_customer_role_playing_eff_hashdiff
                                                )
        SELECT
          l_order_customer_role_playing_hashkey,
          ls_order_customer_role_playing_eff_hashdiff,
          r_timestamp AS r_timestamp,
          LEAD(DATEADD(milliseconds, - 1, r_timestamp), 1, CAST('9999-12-31T00:00:00.000000Z' AS TIMESTAMP)) OVER (PARTITION BY h_customer_role_playing_hashkey ORDER BY r_timestamp) AS r_timestamp_end,
          r_source
          , dummy_descriptive_field
        FROM staging_satellite_affected_records
        ) AS staging
  ON (satellite.l_order_customer_role_playing_hashkey = staging.l_order_customer_role_playing_hashkey
    AND satellite.r_timestamp = staging.r_timestamp
    AND satellite.r_timestamp >= $min_timestamp_satellite)
  WHEN MATCHED THEN
    UPDATE SET satellite.r_timestamp_end = staging.r_timestamp_end
  WHEN NOT MATCHED
    THEN
    INSERT (l_order_customer_role_playing_hashkey, s_hashdiff, r_timestamp, r_timestamp_end, r_source, dummy_descriptive_field)
      VALUES (
               staging.l_order_customer_role_playing_hashkey,
               staging.ls_order_customer_role_playing_eff_hashdiff,
               staging.r_timestamp,
               staging.r_timestamp_end,
               staging.r_source
               , staging.dummy_descriptive_field);
