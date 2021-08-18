MERGE INTO {target_schema}.{target_table} AS target
  USING (
        SELECT DISTINCT
          {source_fields}
        FROM {staging_schema}.{staging_table}
        ) AS staging ON (target.{target_hashkey_field} = staging.{source_hashkey_field})
  WHEN NOT MATCHED THEN INSERT ({target_fields})
    VALUES ({staging_source_fields});
