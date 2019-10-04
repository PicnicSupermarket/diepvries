from typing import List, Union

from .. import END_OF_TIME, HASH_DELIMITER, UNKNOWN, METADATA_FIELDS
from ..data_vault_field import DataVaultField
from ..driving_key_field import DrivingKeyField

# SQL formulas (formatted strings).

# Formula used to COALESCE each business key to be included in staging table SELECT statement.
ALIASED_BUSINESS_KEY_SQL_TEMPLATE = (
    "COALESCE({{business_key}}, '{unknown_text}') " "AS {{business_key}}"
).format(unknown_text=UNKNOWN)

# Field with alias prepended.
ALIASED_FIELD_SQL_TEMPLATE = "{table_alias}.{field_name}"

# Formula used to COALESCE each business key before concatenating them to produce hashes.
# Data Vault does not accept NULL business keys in a Data Vault model.
BUSINESS_KEY_SQL_TEMPLATE = "COALESCE({{business_key}}, '{unknown_text}')".format(
    unknown_text=UNKNOWN
)

# Formula used to coalesce each child key before concatenating them to produce hashes.
# We default the child key to empty string as there is no default value for them.
CHILD_KEY_SQL_TEMPLATE = "COALESCE(CAST({child_key} AS VARCHAR), '')"

# Formula used to coalesce each descriptive field before concatenating them to produce hashes.
# We default each descriptive field to empty string as there is no default value for them,
# to avoid concatenation with NULL.
DESCRIPTIVE_FIELD_SQL_TEMPLATE = "COALESCE(CAST({descriptive_field} AS VARCHAR), '')"

# SQL expression that should be used to represent the end of times (9999-12-31)
# in SQL query filters.
END_OF_TIME_SQL_TEMPLATE = (
    f"CAST('{END_OF_TIME.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}' AS TIMESTAMP)"
)

# Formula used to calculate HASHDIFF fields. {hashdiff_expression} is the concatenation between
# all business keys plus descriptive_field delimited by HASH_DELIMITER.
# The REGEXP_REPLACE function is needed in order to avoid changes in hashdiffs
# when a new field is added to a satellite.
_HASH_DELIMITER_ESCAPED = HASH_DELIMITER.replace("|", "\\\\|")
HASHDIFF_SQL_TEMPLATE = (
    "MD5(REGEXP_REPLACE({{hashdiff_expression}}, "
    "'({hash_delimiter_escaped}){{{{1,}}}}$', '')) "
    "AS {{hashdiff}}"
).format(hash_delimiter_escaped=_HASH_DELIMITER_ESCAPED)

# Formula used to calculate hashkeys. The {hashkey_expression} is the concatenation of all business
# keys plus child keys (if they exist) delimited by HASH_DELIMITER.
HASHKEY_SQL_TEMPLATE = "MD5({hashkey_expression}) AS {hashkey}"

# JOIN condition template SQL.
JOIN_CONDITION_SQL_TEMPLATE = (
    "{table_1_alias}.{field_name} = {table_2_alias}.{field_name}"
)

# Formula used to calculate r_timestamp_end while populating satellites.
# If we find an existing version for a business key that we are going to INSERT,
# this previous version have to be "closed", with the new timestamp (this execution's
# extraction start timestamp) minus 1 millisecond.
RECORD_END_TIMESTAMP_SQL_TEMPLATE = (
    "LEAD(DATEADD(milliseconds, - 1, {record_start_timestamp}), 1, "
    "{end_of_time}) OVER (PARTITION BY {{key_fields}} "
    "ORDER BY {record_start_timestamp}) AS {record_end_timestamp}"
).format(
    end_of_time=END_OF_TIME_SQL_TEMPLATE,
    record_start_timestamp=METADATA_FIELDS["record_start_timestamp"],
    record_end_timestamp=METADATA_FIELDS["record_end_timestamp"],
)

# Formula used to create the record timestamp in staging table.
# This field will always be equivalent to the start of extraction process.
RECORD_START_TIMESTAMP_SQL_TEMPLATE = (
    "CAST('{{extract_start_timestamp}}' AS TIMESTAMP) AS {record_start_timestamp}"
).format(record_start_timestamp=METADATA_FIELDS["record_start_timestamp"])

# Formula used to create the record source field in staging table. A simple SQL constant aliased.
SOURCE_SQL_TEMPLATE = "'{{source}}' AS {record_source}".format(
    record_source=METADATA_FIELDS["record_source"]
)

# Name of the staging table created in the database. The suffix corresponds to
# the extraction timestamp. This physical name will be used as target while creating
# the staging table, and as source while populating the Data Vault model.
STAGING_PHYSICAL_NAME_SQL_TEMPLATE = "{staging_table}_{staging_table_suffix}"


def format_fields_for_join(
    fields: Union[List[DataVaultField], List[DrivingKeyField]],
    table_1_alias: str,
    table_2_alias: str,
) -> List[str]:
    """
    Get formatted list of field names for SQL JOIN condition.

    Args:
        fields (Union[List[DataVaultField], List[DrivingKeyField]]): Fields to be formatted.
            It accepts both DrivingKeyField and DataVaultField instances.
        table_1_alias (str): alias that should be used in the field on the left side of
            equality sign.
        table_2_alias (str): alias that should be used in the field on the right side of
            equality sign.

    Returns:
        List[str]: field list formatted for SQL JOIN condition.
    """
    return [
        JOIN_CONDITION_SQL_TEMPLATE.format(
            field_name=field.name,
            table_1_alias=table_1_alias,
            table_2_alias=table_2_alias,
        )
        for field in fields
    ]


def format_fields_for_select(
    fields: Union[List[DataVaultField], List[DrivingKeyField]], table_alias: str = None
) -> List[str]:
    """
    Get formatted list of field names for SQL SELECT statement.

    Args:
        fields (Union[List[DataVaultField], List[DrivingKeyField]]): Fields to be formatted.
            It accepts both DrivingKeyField and DataVaultField instances.
        table_alias (str): alias that should be used in the each field.

    Returns:
        List[str]: field list formatted for SQL SELECT clause.

    """
    if table_alias is not None:
        return [
            ALIASED_FIELD_SQL_TEMPLATE.format(
                field_name=field.name, table_alias=table_alias
            )
            for field in fields
        ]
    else:
        return [field.name for field in fields]
