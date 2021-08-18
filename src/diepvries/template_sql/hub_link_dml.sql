MERGE INTO {target_schema}.{target_table} AS target
  USING (
        SELECT
          {source_hashkey_field},
          {non_hashkey_fields_aggregation}
        FROM {staging_schema}.{staging_table}
        GROUP BY {source_hashkey_field}
        ) AS staging ON (target.{target_hashkey_field} = staging.{source_hashkey_field})
  WHEN NOT MATCHED THEN INSERT ({target_hashkey_field}, {target_non_hashkey_fields})
    VALUES (staging.{source_hashkey_field}, {source_non_hashkey_fields});
