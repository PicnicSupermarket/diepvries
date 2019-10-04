import logging
from dataclasses import dataclass, asdict
from functools import lru_cache
from typing import List

import snowflake.connector as database_connection

from . import DESERIALIZERS_DIR
from .. import (
    FixedPrefixLoggerAdapter,
    TableType,
    FIELD_SUFFIX,
    FieldRole,
    TABLE_PREFIXES,
)
from ..data_vault_field import DataVaultField
from ..data_vault_table import DataVaultTable
from ..driving_key_field import DrivingKeyField
from ..hub import Hub
from ..link import Link
from ..satellite import Satellite


@dataclass
class DatabaseConfiguration:
    """
    Class that holds all needed fields to create a Snowflake database connection.
    """

    database: str
    user: str
    password: str
    warehouse: str
    account: str


class SnowflakeDeserializer:
    _target_tables = None
    """
    Class responsible for deserializing a Data Vault model, based on Snowflake system
    metadata tables.

    The deserialization process will consist in converting the list of target table
    names to a list of DataVaultTable instances.

    Each DataVaultTable will have a list of DataVaultField instances
    (representing database table columns).
    """

    def __init__(
        self,
        target_schema: str,
        target_tables: List[str],
        database_configuration: DatabaseConfiguration,
        driving_keys: List[DrivingKeyField] = None,
    ):
        """
        SnowflakeDeserializer instantiation.

        Besides setting __init__ arguments as class attributes, it also creates a Snowflake database
        connection.

        Both target_tables and fields have their own setters (check @target_tables.setter and
        @fields.setter for more detail).

        Args:
            target_schema (str): Schema where the Data Vault model is stored.
            target_tables (List[str]): Names of the tables that should be deserialized.
            database_configuration (DatabaseConfiguration): Holds all properties needed to create a Snowflake
                database connection.
            driving_keys (List[DrivingKeyField]): List of fields that should be used as driving keys in
                current model's effectivity satellites (if applicable).
        """
        self.target_schema = target_schema
        self.target_tables = target_tables
        self.database_name = database_configuration
        self.driving_keys = driving_keys

        # Create Snowflake database connection.
        self.database_connection = database_connection.connect(
            **asdict(database_configuration)
        )

        self._logger = FixedPrefixLoggerAdapter(logging.getLogger(__name__), str(self))

        self._logger.info("Instance of (%s) created.", type(self))

    def __str__(self) -> str:
        """
        Defines the representation of a SnowflakeDeserializer object as a string.
        This helps the tracking of logging events per entity.

        Returns:
            str: Logger string format.
        """
        return f"{type(self).__name__}: database={self.database_name}, target_tables={';'.join(self.target_tables)}"

    @property
    def target_tables(self) -> List[str]:
        """
        Gets target table names list. Needed because we want to lower case the table names
        in property setter.

        Returns:
            List[str]: List of target tables names to deserialize.
        """
        return self._target_tables

    @target_tables.setter
    def target_tables(self, target_tables: List[str]):
        """
        Lowers the case of all table names in target_tables.

        Args:
            target_tables (List[str]): Target table names list, in lower case.
        """
        _target_tables = []
        for table in target_tables:
            _target_tables.append(table.lower())

        self._target_tables = _target_tables

    @property
    @lru_cache(1)
    def fields(self) -> List[DataVaultField]:
        """
        Deserializes all fields present in self.target_tables.

        This deserialization will be done using Snowflake metadata system views.

        In order to assure that only one access to the database is done on each deserialization
        process execution, this method is executed only once (in __init__).

        Returns:
            List[DataVaultField]: List of deserialized fields.
        """
        table_prefixes = TABLE_PREFIXES[TableType.HUB]
        table_prefixes.extend(TABLE_PREFIXES[TableType.LINK])
        table_prefixes.extend(TABLE_PREFIXES[TableType.SATELLITE])

        query_args = {
            "target_schema": self.target_schema,
            "table_list": ",".join([f"'{table}'" for table in self.target_tables]),
            "table_prefixes": ",".join([f"'{prefix}'" for prefix in table_prefixes]),
        }

        model_metadata_sql = (
            (DESERIALIZERS_DIR / "snowflake_model_metadata.sql")
            .read_text()
            .format(**query_args)
        )
        with self.database_connection.cursor() as cursor:
            # Get model properties from database metadata (for all target tables passed as argument).
            fields = []
            cursor.execute(model_metadata_sql)
            for field_definition in cursor:
                field = DataVaultField(
                    parent_table_name=field_definition[0],
                    name=field_definition[1],
                    datatype=field_definition[2],
                    position=field_definition[3],
                    is_mandatory=field_definition[4],
                )
                fields.append(field)

        return fields

    @property
    def deserialized_target_tables(self) -> List[DataVaultTable]:
        """
        Deserializes all target tables passed as argument during instance creation (__init__ method).

        Returns:
            List[DataVaultTable]: List of deserialized target tables.

        Raises:
            RuntimeError: If parent table does not have an hashkey field or a table name
                is not valid (without underscores).
        """
        target_tables = []
        for table in self.target_tables:
            parent_table = None
            try:
                prefix = next(split_part for split_part in table.split("_"))
            except StopIteration:
                raise RuntimeError(
                    f"'{table}': Table name is not valid. No separator detected ('_')"
                )

            # Get parent table name (first field of satellite table, without the suffix _hashkey).
            # This will only be needed for satellites, that need a parent table.
            if prefix in TABLE_PREFIXES[TableType.SATELLITE]:
                try:
                    parent_table = next(
                        field.name
                        for field in self.fields
                        if field.name.endswith(f"_{FIELD_SUFFIX[FieldRole.HASHKEY]}")
                        and field.parent_table_name == table
                    ).replace(f"_{FIELD_SUFFIX[FieldRole.HASHKEY]}", "")
                except StopIteration:
                    raise RuntimeError(
                        (
                            f"'{table}': No field named '{table}_{FIELD_SUFFIX[FieldRole.HASHKEY]}' found"
                        )
                    )
            table_fields = [
                field for field in self.fields if field.parent_table_name == table
            ]

            # Get driving keys for current table (if it has driving keys is considered an
            # effectivity satellite, otherwise it is considered a regular satellite.
            if self.driving_keys:
                driving_keys = [
                    driving_key
                    for driving_key in self.driving_keys
                    if driving_key.parent_table_name == parent_table
                    and driving_key.satellite_name == table
                ]
            else:
                driving_keys = None

            table_args = {
                "schema": self.target_schema,
                "name": table,
                "fields": table_fields,
            }

            if prefix in TABLE_PREFIXES[TableType.LINK]:
                target_link = Link(**table_args)
                target_tables.append(target_link)
            elif prefix in TABLE_PREFIXES[TableType.SATELLITE]:
                target_satellite = Satellite(**table_args, driving_keys=driving_keys)
                target_tables.append(target_satellite)
            elif prefix in TABLE_PREFIXES[TableType.HUB]:
                target_hub = Hub(**table_args)
                target_tables.append(target_hub)
            else:
                raise ValueError(
                    f"'{table}': Table name is not valid (check_allowed prefixes in TABLE_PREFIXES)"
                )

        self._logger.info("Deserialization completed.")

        return target_tables
