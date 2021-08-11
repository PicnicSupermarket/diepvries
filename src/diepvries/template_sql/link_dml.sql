MERGE INTO {target_schema}.{target_table} AS link
    USING (
        SELECT {hashkey_field},
               {non_hashkey_fields_aggregation}
        FROM {staging_schema}.{staging_table}
        GROUP BY {hashkey_field}
    ) AS staging ON ({hashkey_condition})
    WHEN NOT MATCHED THEN INSERT ({hashkey_field}, {non_hashkey_fields})
        VALUES (staging.{hashkey_field}, {staging_non_hashkey_fields});
