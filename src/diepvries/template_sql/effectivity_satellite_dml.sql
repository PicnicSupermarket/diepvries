-- Calculate minimum timestamp that can be affected by the current load.
-- This timestamp is used in the MERGE statement to reduce the number of records scanned, ensuring
-- the usage of the recommended clustering key (r_timestamp :: DATE).
-- If there are no matches between the staging table and the target table, the minimum timestamp is set to the
-- current timestamp minus 4 hours. The four hours are subtracted as a "safety net" to avoid the insertion of
-- duplicate records when the first version of a given driving key is being loaded by two processes running in parallel.
-- This is unlikely to happen, but still better to play it on the safe side.
SET min_timestamp_link = (
                         SELECT
                           DATEADD(HOUR, -4, COALESCE(MIN(l.{record_start_timestamp}), CURRENT_TIMESTAMP()))
                         FROM {target_schema}.{link_table} AS l
                           INNER JOIN {staging_schema}.{staging_table} AS staging
                                      ON ({link_driving_key_condition})
                         );

SET min_timestamp_satellite = (
                              SELECT
                                DATEADD(HOUR, -4, COALESCE(MIN(satellite.{record_start_timestamp}), CURRENT_TIMESTAMP()))
                              FROM {target_schema}.{link_table} AS l
                                INNER JOIN {target_schema}.{target_table} AS satellite
                                           ON (l.{hashkey_field} = satellite.{hashkey_field}
                                             AND satellite.{record_end_timestamp_name} = {end_of_time}
                                             AND l.{record_start_timestamp} >= $min_timestamp_link)
                                INNER JOIN {staging_schema}.{staging_table} AS staging
                                           ON ({link_driving_key_condition})
                              );

MERGE INTO {target_schema}.{target_table} AS satellite
  USING (
        WITH
          filtered_effectivity_satellite AS (
          SELECT
            {link_driving_keys},
            satellite.*
          FROM {staging_schema}.{staging_table} AS staging
            INNER JOIN {target_schema}.{link_table} AS l
                       ON ({link_driving_key_condition}
                         AND l.{record_start_timestamp} >= $min_timestamp_link)
            INNER JOIN {target_schema}.{target_table} AS satellite
                       ON (l.{hashkey_field} = satellite.{hashkey_field}
                         AND satellite.{record_end_timestamp_name} = {end_of_time}
                         AND satellite.{record_start_timestamp} >= $min_timestamp_satellite)
                                            ),
          filtered_staging AS (
          SELECT DISTINCT
            {staging_driving_keys},
            staging.{hashkey_field},
            staging.{staging_hashdiff_field},
            staging.{record_start_timestamp},
            staging.{record_source}
            {staging_descriptive_fields}
          FROM {staging_schema}.{staging_table} AS staging
          WHERE NOT EXISTS (
                           SELECT
                             1
                           FROM filtered_effectivity_satellite AS satellite
                           WHERE {satellite_driving_key_condition}
                             AND satellite.{record_start_timestamp} >= staging.{record_start_timestamp}
                           )
                              ),
          --   Records that will be inserted (don't exist in target table or exist
          --   in the target table but the hashdiff changed). As the r_timestamp is fetched
          --   from the staging table, these records will always be included in the
          --   WHEN NOT MATCHED condition of the MERGE command.
          staging_satellite_affected_records AS (
          SELECT
            {staging_driving_keys},
            staging.{hashkey_field},
            staging.{staging_hashdiff_field},
            staging.{record_start_timestamp},
            staging.{record_source}
            {staging_descriptive_fields}
          FROM filtered_staging AS staging
            LEFT JOIN filtered_effectivity_satellite AS satellite
                      ON ({satellite_driving_key_condition})
          WHERE satellite.{hashkey_field} IS NULL
             OR satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}
          UNION ALL
          --  Records from the target table that will have its r_timestamp_end updated
          --  (hashkey already exists in target table, but hashdiff changed). As the
          --  r_timestamp is fetched from the target table, these records will always be
          --  included in the WHEN MATCHED condition of the MERGE command.
          SELECT
            {satellite_driving_keys},
            satellite.{hashkey_field},
            satellite.{hashdiff_field} AS {staging_hashdiff_field},
            satellite.{record_start_timestamp},
            satellite.{record_source}
            {satellite_descriptive_fields}
          FROM filtered_staging AS staging
            INNER JOIN filtered_effectivity_satellite AS satellite
                       ON ({satellite_driving_key_condition})
          WHERE satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}
                                                )
        SELECT
          {hashkey_field},
          {staging_hashdiff_field},
          {record_start_timestamp} AS {record_start_timestamp},
          {record_end_timestamp_expression},
          {record_source}
          {descriptive_fields}
        FROM staging_satellite_affected_records
        ) AS staging
  ON (satellite.{hashkey_field} = staging.{hashkey_field}
    AND satellite.{record_start_timestamp} = staging.{record_start_timestamp}
    AND satellite.{record_start_timestamp} >= $min_timestamp_satellite)
  WHEN MATCHED THEN
    UPDATE SET satellite.{record_end_timestamp_name} = staging.{record_end_timestamp_name}
  WHEN NOT MATCHED
    THEN
    INSERT ({fields})
      VALUES (
               staging.{hashkey_field},
               staging.{staging_hashdiff_field},
               staging.{record_start_timestamp},
               staging.{record_end_timestamp_name},
               staging.{record_source}
               {staging_descriptive_fields});
