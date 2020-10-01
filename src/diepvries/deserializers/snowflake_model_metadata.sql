/*Query that fetches all needed properties to initialize a StagingTable and all DataVaultTable objects*/
SELECT
  LOWER(dm."tab_name")          AS "parent_table_name",
  LOWER(dm."col_name")          AS "name",
  UPPER(dm."col_datatype")      AS "data_type",
  dm."col_position"             AS "position",
  dm."col_not_null"             AS "is_mandatory",
  dm."col_precision"            AS "precision",
  dm."col_scale"                AS "scale",
  NULLIF(dm."col_length", '-1') AS "length"
FROM cfg.dv_metadata AS dm
WHERE dm."sch_name" = LOWER('{target_schema}')
  AND dm."tab_name" IN ({table_list});
