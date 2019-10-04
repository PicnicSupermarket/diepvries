MERGE INTO {target_schema}.{data_vault_table} AS satellite
USING (
  WITH deduplicated_staging AS (
      SELECT ranked_staging.*
      FROM (
        SELECT staging.{hashkey_field},
          staging.{staging_hashdiff_field},
          staging.{record_start_timestamp},
          ROW_NUMBER() OVER (
            PARTITION BY {staging_driving_keys} ORDER BY staging.{staging_hashdiff_field}
            ) AS rank,
          staging.{record_source},
          {staging_driving_keys}
          {staging_descriptive_fields}
        FROM {staging_schema}.{staging_table} AS staging
        ) AS ranked_staging
      CROSS JOIN (
        SELECT MAX({record_start_timestamp}) AS max_r_timestamp
        FROM {target_schema}.{data_vault_table}
        ) AS max_satellite_timestamp
      WHERE ranked_staging.rank = 1
        AND ranked_staging.{record_start_timestamp} >= COALESCE(max_satellite_timestamp.max_r_timestamp, '1970-01-01 00:00:00')
      ),
    effectivity_satellite AS (
      SELECT {link_driving_keys},
        satellite.*
      FROM {target_schema}.{link_table} AS l
      INNER JOIN {target_schema}.{data_vault_table} AS satellite ON (
          l.{hashkey_field} = satellite.{hashkey_field}
          AND satellite.{record_end_timestamp_name} = {end_of_time}
          )
      INNER JOIN deduplicated_staging AS staging ON ({link_driving_key_condition})
      ),
    staging_satellite_affected_records AS (
      SELECT {staging_driving_keys},
        staging.{hashkey_field},
        staging.{staging_hashdiff_field},
        staging.{record_start_timestamp},
        staging.{record_source}
        {staging_descriptive_fields}
      FROM deduplicated_staging AS staging
      LEFT JOIN effectivity_satellite AS satellite ON ({satellite_driving_key_condition})
      WHERE satellite.{hashkey_field} IS NULL
        OR satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}

      UNION ALL

      SELECT {satellite_driving_keys},
        satellite.{hashkey_field},
        satellite.{hashdiff_field} AS {staging_hashdiff_field},
        satellite.{record_start_timestamp},
        satellite.{record_source}
        {satellite_descriptive_fields}
      FROM deduplicated_staging AS staging
      INNER JOIN effectivity_satellite AS satellite ON ({satellite_driving_key_condition})
      WHERE satellite.{hashdiff_field} <> staging.{staging_hashdiff_field}
      )
  SELECT {hashkey_field},
    {staging_hashdiff_field},
    {record_start_timestamp} AS {record_start_timestamp},
    {record_end_timestamp_expression},
    {record_source}
    {descriptive_fields}
  FROM staging_satellite_affected_records
  ) AS staging
  ON (
      satellite.{hashkey_field} = staging.{hashkey_field}
      AND satellite.{record_start_timestamp} = staging.{record_start_timestamp}
      )
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
      {staging_descriptive_fields}
      );
