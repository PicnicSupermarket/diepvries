"""Definitions and helpers."""

import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


class FieldDataType(Enum):
    """Possible data types for a Field.

    These data types are mapped directly from Snowflake root data types (the ones
    that are actually stored in Snowflake metadata views).
    """

    ARRAY = "ARRAY"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    GEOGRAPHY = "GEOGRAPHY"
    NUMBER = "NUMBER"
    OBJECT = "OBJECT"
    REAL = "REAL"
    TEXT = "TEXT"
    TIME = "TIME"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    VARIANT = "VARIANT"


class FieldRole(Enum):
    """Possible roles for each field in a Data Vault model."""

    HASHKEY = "hashkey"
    HASHKEY_PARENT = "hashkey_parent"
    HASHDIFF = "hashdiff"
    BUSINESS_KEY = "business_key"
    CHILD_KEY = "child_key"
    DRIVING_KEY = "driving_key"
    DESCRIPTIVE = "descriptive"
    METADATA = "metadata"


class TableType(Enum):
    """Possible types of a table in a Data Vault model."""

    HUB = "hub"
    LINK = "link"
    SATELLITE = "satellite"


class FixedPrefixLoggerAdapter(logging.LoggerAdapter):
    """Logger with a prefix.

    This LoggerAdapter implementation prefixes log calls with "[prefix]".

    This is useful to add contextually valuable information to the log call
    (Snowflake username and session ID) without changing the logging format of the
    root logger.
    """

    def __init__(self, logger, prefix):
        """Instantiate logger."""
        super().__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        """Process a message."""
        # pylint: disable=consider-using-f-string
        return "[(%s)] (%s)" % (self.prefix, msg), kwargs


# Framework constants.

# Timestamp used to populate r_timestamp_end in new satellite records.
END_OF_TIME = datetime(9999, 12, 31, tzinfo=timezone.utc)

# Possible values for a field suffix, by field role (when applicable).
FIELD_SUFFIX = {
    FieldRole.HASHKEY: "hashkey",
    FieldRole.HASHKEY_PARENT: "hashkey",
    FieldRole.BUSINESS_KEY: "id",
    FieldRole.HASHDIFF: "hashdiff",
}

# Possible values for a field prefix, by field role (when applicable).
FIELD_PREFIX = {
    FieldRole.CHILD_KEY: "ck",
    FieldRole.METADATA: "r",
    FieldRole.HASHKEY: "h",
}

# Delimiter used in the concatenation of fields to calculate hashkeys/hashdiffs.
HASH_DELIMITER = "|~~|"

# Dictionary with all metadata fields allowed in Data Vault model.
METADATA_FIELDS = {
    "record_source": "r_source",
    "record_start_timestamp": "r_timestamp",
    "record_end_timestamp": "r_timestamp_end",
}

# Path object that stores the path of this file's parent folder.
PROJECT_DIR = Path(__file__).resolve().parent

# Possible table prefixes, by table type.
TABLE_PREFIXES = {
    TableType.HUB: ["h"],
    TableType.LINK: ["l"],
    TableType.SATELLITE: ["hs", "ls"],
}

# Path object that stores the path to the SQL templates folder.
TEMPLATES_DIR = PROJECT_DIR / "template_sql"

# String used as default for NULL business keys (from extract table to staging table).
UNKNOWN = "dv_unknown"
