MERGE INTO {target_schema}.{target_table} AS satellite
  USING (
        WITH
          filtered_satellite AS (
          SELECT
            satellite.{hashkey_field},
            satellite.{hashdiff_field},
            satellite.{record_start_timestamp},
            satellite.{record_source}
            {satellite_descriptive_fields}
          FROM {target_schema}.{target_table} AS satellite
            INNER JOIN {staging_schema}.{staging_table} AS staging
                       ON (satellite.{hashkey_field} = staging.{hashkey_field}
                         AND satellite.{record_end_timestamp_name} = {end_of_time})
                                ),
          filtered_staging AS (
          SELECT DISTINCT
            staging.{hashkey_field},
            staging.{staging_hashdiff_field},
            staging.{record_start_timestamp},
            staging.{record_source}
            {staging_descriptive_fields}
          FROM {staging_schema}.{staging_table} AS staging
            LEFT JOIN filtered_satellite AS satellite
                      ON (staging.{hashkey_field} = satellite.{hashkey_field})
          WHERE satellite.{hashkey_field} IS NULL
             OR satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}
                              ),
          --  Records that will be inserted (don't exist in target table or exist
          --  in the target table but the hashdiff changed). As the r_timestamp is fetched
          --  from the staging table, these records will always be included in the
          --  WHEN NOT MATCHED condition of the MERGE command.
          staging_satellite_affected_records AS (
          SELECT
            staging.{hashkey_field},
            staging.{staging_hashdiff_field},
            staging.{record_start_timestamp},
            staging.{record_source}
            {staging_descriptive_fields}
          FROM filtered_staging AS staging
          UNION ALL
          -- Records from the target table that will have its r_timestamp_end updated
          -- (hashkey already exists in target table, but hashdiff changed). As the
          -- r_timestamp is fetched from the target table, these records will always be
          -- included in the WHEN MATCHED condition of the MERGE command.
          SELECT
            satellite.{hashkey_field},
            satellite.{hashdiff_field},
            satellite.{record_start_timestamp},
            satellite.{record_source}
            {satellite_descriptive_fields}
          FROM filtered_satellite AS satellite
            INNER JOIN filtered_staging AS staging
                       ON (staging.{hashkey_field} = satellite.{hashkey_field})
                                                )
        SELECT
          {hashkey_field},
          {staging_hashdiff_field},
          {record_start_timestamp},
          {record_end_timestamp_expression},
          {record_source}
          {descriptive_fields}
        FROM staging_satellite_affected_records
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
