"""Deserializer for Snowflake."""

import json
import logging
from collections import defaultdict
from dataclasses import asdict, dataclass
from functools import cached_property
from typing import Dict, List, Optional, Type

from snowflake.connector import DictCursor, connect
from snowflake.connector.network import DEFAULT_AUTHENTICATOR

from .. import TABLE_PREFIXES, FieldDataType, FixedPrefixLoggerAdapter, TableType
from ..driving_key_field import DrivingKeyField
from ..effectivity_satellite import EffectivitySatellite
from ..field import Field
from ..hub import Hub
from ..link import Link
from ..role_playing_hub import RolePlayingHub
from ..satellite import Satellite
from ..table import DataVaultTable
from . import DESERIALIZERS_DIR

METADATA_SQL_FILE_PATH = DESERIALIZERS_DIR / "snowflake_model_metadata.sql"


@dataclass
class DatabaseConfiguration:
    """Needed fields to create a Snowflake database connection."""

    database: str
    user: str
    warehouse: str
    account: str
    password: Optional[str] = None
    authenticator: Optional[str] = DEFAULT_AUTHENTICATOR

    def __post_init__(self):
        """Validate input for optional attributes."""
        if self.authenticator == DEFAULT_AUTHENTICATOR and not self.password:
            raise ValueError(
                f"Password was not provided. It is a mandatory attribute when the "
                f"authenticator is not `{DEFAULT_AUTHENTICATOR}`. Empty passwords are "
                f"only allowed when `authenticator='externalbrowser'`."
            )


class SnowflakeDeserializer:
    """Deserialize a Data Vault model, based on Snowflake system metadata tables.

    The deserialization process will consist in converting the list of target table
    names to a list of Table instances.

    Each Table will have a list of Field instances (representing
    database table columns).
    """

    def __init__(
        self,
        target_schema: str,
        target_tables: List[str],
        database_configuration: DatabaseConfiguration,
        driving_keys: List[DrivingKeyField] = None,
        role_playing_hubs: Dict[str, str] = None,
    ):
        """Instantiate a SnowflakeDeserializer.

        Besides setting __init__ arguments as class attributes, it also creates a
        Snowflake database connection.

        Both target_tables and fields have their own setters (check
        @target_tables.setter and @fields.setter for more detail).

        Args:
            target_schema: Schema where the Data Vault model is stored.
            target_tables: Names of the tables that should be deserialized.
            database_configuration: Holds all properties needed to create a Snowflake
                database connection.
            driving_keys: List of fields that should be used as driving keys in
                current model's effectivity satellites (if applicable).
            role_playing_hubs: List of tables that should be created as
                RolePlayingHub objects. Each dictionary has the role playing hub as key
                and the parent table as value.
        """
        self.target_schema = target_schema
        self.target_tables = [table.lower() for table in target_tables]
        self.target_database = database_configuration.database
        self.driving_keys = driving_keys or []
        self.role_playing_hubs = role_playing_hubs or {}

        # Create Snowflake database connection.
        self.database_connection = connect(**asdict(database_configuration))

        self._logger = FixedPrefixLoggerAdapter(logging.getLogger(__name__), str(self))

        self._logger.info("Instance of (%s) created.", type(self))

    def __str__(self) -> str:
        """Representation of a SnowflakeDeserializer object as a string.

        This helps the tracking of logging events per entity.

        Returns:
            Logger string format.
        """
        return (
            f"{type(self).__name__}: database={self.target_database}, "
            f"target_tables={';'.join(self.target_tables)}"
        )

    def _deserialize_table(self, target_table_name: str) -> DataVaultTable:
        """Instantiate a DataVault table.

        Args:
            target_table_name: Name of the table to be instantiated.

        Returns:
            Deserialized table.
        """
        table_args = {
            "schema": self.target_schema,
            "name": target_table_name,
            "fields": self._fields[target_table_name],
        }
        if self._get_table_type(target_table_name) == EffectivitySatellite:
            table_args["driving_keys"] = self._driving_keys_by_table.get(
                target_table_name
            )

        return self._get_table_type(target_table_name)(**table_args)

    @cached_property
    def _driving_keys_by_table(self) -> Dict[str, List[DrivingKeyField]]:
        """Get mapping between a satellite and its driving keys.

         The table must exist in self.driving_keys.

        Returns:
            List of driving keys, indexed by table name.
        """
        driving_keys_by_table = {}
        effectivity_satellites = {
            driving_key.satellite_name for driving_key in self.driving_keys
        }
        for table in effectivity_satellites:
            driving_keys_by_table[table] = [
                driving_key
                for driving_key in self.driving_keys
                if driving_key.satellite_name == table
            ]

        return driving_keys_by_table

    @cached_property
    def _fields(self) -> Dict[str, List[Field]]:
        """Deserialize all fields present in `self.target_tables`.

        This deserialization will be done using Snowflake's `SHOW COLUMNS` command.

        Returns:
            Mapping between each table and its fields list.
        """
        if self.role_playing_hubs:
            tables = set(self.target_tables + list(self.role_playing_hubs.values()))
        else:
            tables = self.target_tables

        model_metadata_sql = METADATA_SQL_FILE_PATH.read_text().format(
            target_database=self.target_database, target_schema=self.target_schema
        )
        with self.database_connection.cursor(DictCursor) as cursor:
            # Get model properties from database metadata (for all tables in
            # self.target_tables).
            cursor.execute(model_metadata_sql)
            fields = defaultdict(list)

            # Variables used to calculate the position of each field within its table.
            # Snowflake's `SHOW COLUMNS` command returns the columns' metadata in the
            # correct order, but does not return a pre-calculated field with the
            # position of the field.
            previous_table = None
            position = 1

            for field in cursor:
                table_name = field["table_name"].lower()

                if table_name not in tables:
                    continue

                if previous_table != table_name:
                    position = 1

                data_type_properties = json.loads(field["data_type"])

                fields[table_name].append(
                    Field(
                        parent_table_name=table_name,
                        name=field["column_name"].lower(),
                        data_type=FieldDataType(
                            data_type_properties["type"]
                            if data_type_properties["type"] != "FIXED"
                            else "NUMBER"
                        ),
                        position=position,
                        is_mandatory=not (data_type_properties["nullable"]),
                        precision=data_type_properties.get("precision"),
                        scale=data_type_properties.get("scale"),
                        length=data_type_properties.get("length"),
                    )
                )

                position += 1
                previous_table = table_name

        return fields

    def _get_table_type(self, target_table_name: str) -> Type[DataVaultTable]:
        """Get the type (class) that should be used to instantiate a given target table.

        The type is calculated based on the table prefix.

        Args:
            target_table_name: Name of the table.

        Returns:
            Mapping between the table name and table type.

        Raises:
            RuntimeError: When the table name is not valid (does not have a valid
                prefix).
        """
        table_prefix = next(split_part for split_part in target_table_name.split("_"))
        if (
            table_prefix in TABLE_PREFIXES[TableType.HUB]
            and target_table_name in self.role_playing_hubs.keys()
        ):
            return RolePlayingHub
        if table_prefix in TABLE_PREFIXES[TableType.HUB]:
            return Hub
        if table_prefix in TABLE_PREFIXES[TableType.LINK]:
            return Link
        if table_prefix in TABLE_PREFIXES[
            TableType.SATELLITE
        ] and self._driving_keys_by_table.get(target_table_name):
            return EffectivitySatellite
        if table_prefix in TABLE_PREFIXES[TableType.SATELLITE]:
            return Satellite

        raise RuntimeError(
            f"'{target_table_name}' is not a valid name for a Table "
            f"(check allowed prefixes in TABLE_PREFIXES enum)"
        )

    @property
    def deserialized_target_tables(self) -> List[DataVaultTable]:
        """Deserialize all target tables passed as argument during instance creation.

        Returns:
            List of deserialized target tables.
        """
        deserialized_target_tables = [
            self._deserialize_table(table) for table in self.target_tables
        ]

        # Set RolePlayingHubs parent tables.
        role_playing_hubs = filter(
            lambda x: isinstance(x, RolePlayingHub), deserialized_target_tables
        )
        for rph in role_playing_hubs:
            rph.parent_table = self._deserialize_table(self.role_playing_hubs[rph.name])

        return deserialized_target_tables
