INSERT INTO {target_schema}.{data_vault_table} ({parent_hashkey_field}, {parent_non_hashkey_fields})
  SELECT {hashkey_field},
      {non_hashkey_fields_aggregation}
  FROM {staging_schema}.{staging_table} AS staging
  WHERE NOT EXISTS(SELECT 1
                   FROM {target_schema}.{data_vault_table} AS hub
                   WHERE {business_key_condition})
  GROUP BY {hashkey_field};
