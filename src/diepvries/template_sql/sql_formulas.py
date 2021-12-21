"""SQL formulas (formatted strings)."""

from typing import List, Union

from .. import END_OF_TIME, HASH_DELIMITER, METADATA_FIELDS, UNKNOWN
from ..driving_key_field import DrivingKeyField
from ..field import Field

# Formula used to COALESCE each business key to be included in staging table SELECT
# statement.
ALIASED_BUSINESS_KEY_SQL_TEMPLATE = (
    f"COALESCE({{business_key}}, '{UNKNOWN}') AS {{business_key}}"
)

# Field with alias prepended.
ALIASED_FIELD_SQL_TEMPLATE = "{table_alias}.{field_name}"

# SQL expression that should be used to represent the end of times (9999-12-31)
# in SQL query filters.
END_OF_TIME_SQL_TEMPLATE = (
    f"CAST('{END_OF_TIME.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}' AS TIMESTAMP)"
)

# Formula used to calculate HASHDIFF fields. {hashdiff_expression} is the concatenation
# between all business keys plus descriptive_field delimited by HASH_DELIMITER.
# The REGEXP_REPLACE function is needed in order to avoid changes in hashdiffs
# when a new field is added to a satellite.
_HASH_DELIMITER_ESCAPED = HASH_DELIMITER.replace("|", "\\\\|")
HASHDIFF_SQL_TEMPLATE = (
    f"MD5(REGEXP_REPLACE({{hashdiff_expression}}, "
    f"'({_HASH_DELIMITER_ESCAPED})+$', '')) "
    f"AS {{hashdiff}}"
)

# Formula used to calculate hashkeys. The {hashkey_expression} is the concatenation of
# all business keys plus child keys (if they exist) delimited by HASH_DELIMITER.
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
    f"LEAD(DATEADD(milliseconds, - 1, {METADATA_FIELDS['record_start_timestamp']}), 1, "
    f"{END_OF_TIME_SQL_TEMPLATE}) OVER (PARTITION BY {{key_fields}} "
    f"ORDER BY {METADATA_FIELDS['record_start_timestamp']}) AS "
    f"{METADATA_FIELDS['record_end_timestamp']}"
)

# Formula used to create the record timestamp in staging table.
# This field will always be equivalent to the start of extraction process.
RECORD_START_TIMESTAMP_SQL_TEMPLATE = (
    f"CAST('{{extract_start_timestamp}}' AS TIMESTAMP) AS "
    f"{METADATA_FIELDS['record_start_timestamp']}"
)

# Formula used to create the record source field in staging table. A simple SQL constant
# aliased.
SOURCE_SQL_TEMPLATE = f"'{{source}}' AS {METADATA_FIELDS['record_source']}"


def format_fields_for_join(
    fields: List[Union[Field, DrivingKeyField]],
    table_1_alias: str,
    table_2_alias: str,
) -> List[str]:
    """Get formatted list of field names for SQL JOIN condition.

    Args:
        fields: Fields to be formatted.
        table_1_alias: Alias that should be used in the field on the left side of the
            equality sign.
        table_2_alias: alias that should be used in the field on the right side of the
            equality sign.

    Returns:
        Fields list formatted for an SQL JOIN condition.
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
    fields: List[Union[Field, DrivingKeyField]], table_alias: str = None
) -> List[str]:
    """Get formatted list of field names for SQL SELECT statement.

    Args:
        fields: Fields to be formatted.
        table_alias: Alias that should be used in each field.

    Returns:
        Fields list formatted for SQL SELECT clause.

    """
    if table_alias is not None:
        return [
            ALIASED_FIELD_SQL_TEMPLATE.format(
                field_name=field.name, table_alias=table_alias
            )
            for field in fields
        ]

    return [field.name for field in fields]
