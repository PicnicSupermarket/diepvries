INSERT INTO {target_schema}.{data_vault_table} ({fields})
  SELECT DISTINCT {fields}
  FROM {staging_schema}.{staging_table} AS staging
  WHERE NOT EXISTS(SELECT 1
                   FROM {target_schema}.{data_vault_table} AS lnk
                   WHERE {hashkey_condition});
