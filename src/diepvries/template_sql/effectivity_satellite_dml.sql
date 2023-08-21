MERGE INTO {target_schema}.{target_table} AS satellite
  USING (
        WITH
          effectivity_satellite AS (
          SELECT
            {link_driving_keys},
            satellite.*
          FROM {target_schema}.{link_table} AS l
            INNER JOIN {target_schema}.{target_table} AS satellite
                       ON (l.{hashkey_field} = satellite.{hashkey_field}
                         AND satellite.{record_end_timestamp_name} = {end_of_time})
                                   ),
          -- Calculate minimum record timestamp for records that might be affected in the
          -- target satellite. This record timestamp will be used to filter the target satellite,
          -- ensuring the recommended cluster key (r_timestamp :: DATE) on the satellite is used,
          -- massively reducing the number of records scanned.
          min_r_timestamp AS (
          SELECT
            COALESCE(MIN({record_start_timestamp}), CURRENT_TIMESTAMP()) AS min_r_timestamp
          FROM effectivity_satellite AS satellite
          WHERE {record_end_timestamp_name} = {end_of_time}
            AND EXISTS(
                      SELECT
                        1
                      FROM {staging_schema}.{staging_table} AS staging
                      WHERE {satellite_driving_key_condition}
                        AND staging.{staging_hashdiff_field} <> satellite.{hashdiff_field}
                      )
                             ),
          -- Filter the target satellite to include only records that might be affected
          -- during the current load. This filter will be specially effective if the target
          -- satellite is clustered using r_timestamp :: DATE.
          filtered_effectivity_satellite AS (
          SELECT *
          FROM effectivity_satellite
          WHERE {record_start_timestamp} >= (
                                              SELECT
                                                min_r_timestamp
                                              FROM min_r_timestamp
                                              )
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
                             AND satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}
                           )
                              )
        --   Records that will be inserted (don't exist in target table or exist
        --   in the target table but the hashdiff changed). As the r_timestamp is fetched
        --   from the staging table, these records will always be included in the
        --   WHEN NOT MATCHED condition of the MERGE command.
        SELECT
          {staging_driving_keys},
          staging.{hashkey_field},
          staging.{staging_hashdiff_field},
          staging.{record_start_timestamp},
          {end_of_time} AS {record_end_timestamp_name},
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
          satellite.{hashdiff_field}                                AS {staging_hashdiff_field},
          satellite.{record_start_timestamp},
          DATEADD(NANOSECOND, -1, staging.{record_start_timestamp}) AS {record_end_timestamp_name},
          satellite.{record_source}
          {satellite_descriptive_fields}
        FROM filtered_staging AS staging
          INNER JOIN filtered_effectivity_satellite AS satellite
                     ON ({satellite_driving_key_condition})
        WHERE satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}
        ) AS staging
  ON (satellite.{hashkey_field} = staging.{hashkey_field}
    AND satellite.{record_start_timestamp} = staging.{record_start_timestamp})
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
