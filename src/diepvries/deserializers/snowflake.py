import logging
from dataclasses import asdict, dataclass
from functools import lru_cache
from typing import Dict, List, Union

from snowflake.connector import DictCursor, connect

from .. import TABLE_PREFIXES, FieldDataType, FixedPrefixLoggerAdapter, TableType
from ..data_vault_field import DataVaultField
from ..data_vault_table import DataVaultTable
from ..driving_key_field import DrivingKeyField
from ..effectivity_satellite import EffectivitySatellite
from ..hub import Hub
from ..link import Link
from ..role_playing_hub import RolePlayingHub
from ..satellite import Satellite
from . import DESERIALIZERS_DIR


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
        role_playing_hubs: Dict[str, str] = None,
    ):
        """
        SnowflakeDeserializer instantiation.

        Besides setting __init__ arguments as class attributes, it also creates a
        Snowflake database connection.

        Both target_tables and fields have their own setters (check
        @target_tables.setter and @fields.setter for more detail).

        Args:
            target_schema (str): Schema where the Data Vault model is stored.
            target_tables (List[str]): Names of the tables that should be deserialized.
            database_configuration (DatabaseConfiguration): Holds all properties needed
                to create a Snowflake database connection.
            driving_keys (List[DrivingKeyField]): List of fields that should be used
                as driving keys in current model's effectivity satellites (if
                applicable).
            role_playing_hubs (Dict[str, str]): List of tables that should be created
                as RolePlayingHub objects. Each dictionary has the role playing hub as
                key and the parent table as value.
        """
        self.target_schema = target_schema
        self.target_tables = target_tables
        self.database_name = database_configuration
        self.driving_keys = driving_keys or []
        self.role_playing_hubs = role_playing_hubs or []

        # Create Snowflake database connection.
        self.database_connection = connect(**asdict(database_configuration))

        self._logger = FixedPrefixLoggerAdapter(logging.getLogger(__name__), str(self))

        self._logger.info("Instance of (%s) created.", type(self))

    def __str__(self) -> str:
        """
        Define the representation of a SnowflakeDeserializer object as a string.
        This helps the tracking of logging events per entity.

        Returns:
            str: Logger string format.
        """
        return (
            f"{type(self).__name__}: database={self.database_name}, "
            f"target_tables={';'.join(self.target_tables)}"
        )

    def _deserialize_table(self, target_table_name: str) -> Union[Hub, Link, Satellite]:
        """
        Instantiate a DataVault table.

        Args:
            target_table_name (str): Name of the table to be instantiated.

        Returns:
            Union[Hub, Link, Satellite]: Deserialized table.
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

    @property
    @lru_cache(1)
    def _driving_keys_by_table(self) -> Dict[str, List[DrivingKeyField]]:
        """
        Produce a mapping between a satellite and its driving keys (if the table exists
        in self.driving_keys).

        Returns:
            Dict[str, List[DrivingKeyField]]: List of driving keys, indexed by table
                name.
        """
        driving_keys_by_table = {}
        effectivity_satellites = set(
            [driving_key.satellite_name for driving_key in self.driving_keys]
        )
        for table in effectivity_satellites:
            driving_keys_by_table[table] = [
                driving_key
                for driving_key in self.driving_keys
                if driving_key.satellite_name
            ]

        return driving_keys_by_table

    @property
    @lru_cache(1)
    def _fields(self) -> Dict[str, List[DataVaultField]]:
        """
        Deserialize all fields present in self.target_tables.

        This deserialization will be done using Snowflake metadata system views.

        Returns:
            Dict[str, List[DataVaultField]]: Mapping between each table and its fields
                list.
        """
        if self.role_playing_hubs:
            tables = set(self.target_tables + list(self.role_playing_hubs.values()))
        else:
            tables = self.target_tables

        query_args = {
            "target_schema": self.target_schema,
            "table_list": ",".join([f"'{table}'" for table in tables]),
        }

        model_metadata_sql = (
            (DESERIALIZERS_DIR / "snowflake_model_metadata.sql")
            .read_text()
            .format(**query_args)
        )
        with self.database_connection.cursor(DictCursor) as cursor:
            # Get model properties from database metadata (for all target
            # in self.target_tables).
            cursor.execute(model_metadata_sql)
            fields = {}
            for field in cursor:
                field["data_type"] = FieldDataType(field["data_type"])

                if fields.get(field["parent_table_name"]):
                    fields[field["parent_table_name"]].append(DataVaultField(**field))
                else:
                    fields[field["parent_table_name"]] = [DataVaultField(**field)]

        return fields

    def _get_table_type(self, target_table_name: str) -> type:
        """
        Get the type (class) that should be used to instantiate a given target table.

        The type is calculated based on the table prefix.

        Args:
            target_table_name (str): Name of the table.

        Returns:
            Dict[str, TableType]: Mapping between the table name and table type.

        Raises:
            RuntimeError: When the table name is not valid (does not have a valid prefix).
        """
        table_prefix = next(split_part for split_part in target_table_name.split("_"))
        role_playing_hubs = self.role_playing_hubs or {}
        if (
            table_prefix in TABLE_PREFIXES[TableType.HUB]
            and target_table_name in role_playing_hubs.keys()
        ):
            return RolePlayingHub
        elif table_prefix in TABLE_PREFIXES[TableType.HUB]:
            return Hub
        elif table_prefix in TABLE_PREFIXES[TableType.LINK]:
            return Link
        elif table_prefix in TABLE_PREFIXES[
            TableType.SATELLITE
        ] and self._driving_keys_by_table.get(target_table_name):
            return EffectivitySatellite
        elif table_prefix in TABLE_PREFIXES[TableType.SATELLITE]:
            return Satellite
        else:
            raise RuntimeError(
                f"'{target_table_name}' is not a valid name for a "
                f"DataVaultTable (check allowed prefixes in "
                f"TABLE_PREFIXES enum)"
            )

    @property
    def deserialized_target_tables(self) -> List[DataVaultTable]:
        """
        Deserialize all target tables passed as argument during instance creation
        (__init__ method).

        Returns:
            List[DataVaultTable]: List of deserialized target tables.
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

    @property
    def target_tables(self) -> List[str]:
        """
        Get target table names list. Needed because we want to lower case the table
        names in property setter.

        Returns:
            List[str]: List of target tables names to deserialize.
        """
        return self._target_tables

    @target_tables.setter
    def target_tables(self, target_tables: List[str]):
        """
        Lower the case of all table names in target_tables.

        Args:
            target_tables (List[str]): Target table names list, in lower case.
        """
        self._target_tables = [table.lower() for table in target_tables]
