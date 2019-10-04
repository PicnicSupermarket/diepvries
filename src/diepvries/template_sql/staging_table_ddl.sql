CREATE OR REPLACE TABLE {staging_schema}.{staging_table}
  ({fields_ddl}) AS
  SELECT {fields_dml}
  FROM {extract_schema_name}.{extract_table_name};
