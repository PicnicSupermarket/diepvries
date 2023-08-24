SET min_timestamp = (
                    SELECT
                      COALESCE(MIN(target.{record_start_timestamp}), CURRENT_TIMESTAMP())
                    FROM {staging_schema}.{staging_table} AS staging
                      INNER JOIN {target_schema}.{target_table} AS target
                                 ON (staging.{source_hashkey_field} = target.{target_hashkey_field})
                    );

MERGE INTO {target_schema}.{target_table} AS target
  USING (
        SELECT DISTINCT
          {source_hashkey_field},
          -- If multiple sources for the same hashkey are received, their values
          -- are concatenated using a comma.
          LISTAGG(DISTINCT {record_source_field}, ',')
                  WITHIN GROUP (ORDER BY {record_source_field})
                  OVER (PARTITION BY {source_hashkey_field}) AS {record_source_field},
          {source_fields}
        FROM {staging_schema}.{staging_table}
        ) AS staging ON (target.{target_hashkey_field} = staging.{source_hashkey_field}
    AND target.{record_start_timestamp} >= $min_timestamp)
  WHEN NOT MATCHED THEN INSERT ({target_fields})
    VALUES ({staging_source_fields});
