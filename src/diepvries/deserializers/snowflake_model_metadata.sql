/*Query that fetches all needed properties to initialize a StagingTable and all DataVaultTable objects*/
SELECT
  LOWER(columns.table_name)                   AS "parent_table_name",
  LOWER(columns.column_name)                  AS "name",
  UPPER(columns.data_type)                    AS "data_type",
  columns.ordinal_position::INTEGER           AS "position",
  NOT (TO_BOOLEAN(columns.is_nullable))       AS "is_mandatory",
  columns.numeric_precision::INTEGER          AS "precision",
  columns.numeric_scale::INTEGER              AS "scale",
  columns.character_maximum_length::INTEGER   AS "length"
FROM information_schema.columns AS columns
WHERE columns.table_schema = UPPER('{target_schema}')
  AND LOWER(table_name) IN ({table_list});
