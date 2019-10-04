import logging
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path


class FieldRole(Enum):
    """
    Defines the possible roles for each field in a Data Vault model.
    """

    HASHKEY = "hashkey"
    HASHKEY_PARENT = "hashkey_parent"
    HASHDIFF = "hashdiff"
    BUSINESS_KEY = "business_key"
    CHILD_KEY = "child_key"
    DRIVING_KEY = "driving_key"
    DESCRIPTIVE = "descriptive"
    METADATA = "metadata"


class TableType(Enum):
    """
    Defines the possible types of table in a Data Vault model.
    """

    HUB = "hub"
    LINK = "link"
    SATELLITE = "satellite"


class FixedPrefixLoggerAdapter(logging.LoggerAdapter):
    """
    This LoggerAdapter implementation prefixes log calls with "[prefix]".
    This is useful to add contextually valuable information to the log call (Snowflake username
    and session ID) without changing the logging format of the root logger.

    XXX: remove code and replace references to picnic tools logger
        (when this class is migrated to picnic tools).
    """

    def __init__(self, logger, prefix):
        super().__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
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
FIELD_PREFIX = {FieldRole.CHILD_KEY: "ck", FieldRole.METADATA: "r"}

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
