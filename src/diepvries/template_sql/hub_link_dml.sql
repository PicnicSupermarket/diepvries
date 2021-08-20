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
        ) AS staging ON (target.{target_hashkey_field} = staging.{source_hashkey_field})
  WHEN NOT MATCHED THEN INSERT ({target_fields})
    VALUES ({staging_source_fields});
