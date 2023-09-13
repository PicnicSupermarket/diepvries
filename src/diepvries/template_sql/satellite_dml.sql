-- Calculate minimum timestamp that can be affected by the current load.
-- This timestamp is used in the MERGE statement to reduce the number of records scanned, ensuring
-- the usage of the recommended clustering key (r_timestamp :: DATE).
-- If there are no matches between the staging table and the target table, the minimum timestamp is set to the
-- current timestamp minus 4 hours. The four hours are subtracted as a "safety net" to avoid the insertion of
-- duplicate records when the first version of a given hashkey is being loaded by two processes running in parallel.
-- This is unlikely to happen, but still better to play it on the safe side.
SET min_timestamp = (
                    SELECT
                      DATEADD(HOUR, -4, COALESCE(MIN(satellite.{record_start_timestamp}), CURRENT_TIMESTAMP()))
                    FROM {staging_schema}.{staging_table} AS staging
                      INNER JOIN {target_schema}.{target_table} AS satellite
                                 ON (satellite.{hashkey_field} = staging.{hashkey_field}
                                   AND satellite.{record_end_timestamp_name} = {end_of_time})
                    );

MERGE INTO {target_schema}.{target_table} AS satellite
  USING (
        WITH
          filtered_satellite AS (
          SELECT *
          FROM {target_schema}.{target_table}
          WHERE {record_end_timestamp_name} = {end_of_time}
            AND {record_start_timestamp} >= $min_timestamp
                                ),
          filtered_staging AS (
          SELECT DISTINCT
            staging.{hashkey_field},
            staging.{staging_hashdiff_field},
            staging.{record_start_timestamp},
            staging.{record_source}
            {staging_descriptive_fields}
          FROM {staging_schema}.{staging_table} AS staging
          WHERE NOT EXISTS (
                           SELECT
                             1
                           FROM filtered_satellite AS satellite
                           WHERE staging.{hashkey_field} = satellite.{hashkey_field}
                             AND satellite.{record_start_timestamp} >= staging.{record_start_timestamp}
                           )
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
            LEFT OUTER JOIN filtered_satellite AS satellite
                            ON (staging.{hashkey_field} = satellite.{hashkey_field}
                              AND satellite.{record_end_timestamp_name} = {end_of_time})
          WHERE satellite.{hashkey_field} IS NULL
             OR satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}
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
                       ON (staging.{hashkey_field} = satellite.{hashkey_field}
                         AND satellite.{record_end_timestamp_name} = {end_of_time})
          WHERE staging.{staging_hashdiff_field} <> satellite.{hashdiff_field}
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
    AND satellite.{record_start_timestamp} >= $min_timestamp)
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
