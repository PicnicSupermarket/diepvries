"""Unit tests for SnowflakeDeserializer."""

from typing import Dict, List
from unittest import mock
from unittest.mock import MagicMock, PropertyMock

import pytest
from snowflake.connector.cursor import SnowflakeCursor

from diepvries.deserializers.snowflake_deserializer import (
    DatabaseConfiguration,
    SnowflakeDeserializer,
)
from diepvries.driving_key_field import DrivingKeyField
from diepvries.effectivity_satellite import EffectivitySatellite
from diepvries.field import Field
from diepvries.hub import Hub
from diepvries.link import Link
from diepvries.role_playing_hub import RolePlayingHub
from diepvries.satellite import Satellite
from diepvries.table import Table

# pylint: disable=protected-access


def compare_tables(test_table: Table, expected_table: Table):
    """Compare two Data Vault tables.

    Two tables are considered equal if they:

    - Have the same name.
    - Have the same type.
    - Have the same fields.
    """
    assert test_table.name == expected_table.name
    # pylint: disable=unidiomatic-typecheck
    assert type(test_table) == type(expected_table)
    assert test_table.fields == expected_table.fields


def test_deserialize_table(
    snowflake_deserializer: SnowflakeDeserializer,
    fields: Dict[str, List[Field]],
    driving_keys_by_table: Dict[str, List[DrivingKeyField]],
    h_customer: Hub,
    h_customer_role_playing: RolePlayingHub,
    l_order_customer: Link,
    hs_customer: Satellite,
    ls_order_customer_eff: EffectivitySatellite,
):
    """Test `SnowflakeDeserializer._deserialize_table` method.

    In order to isolate the behaviour of this method, `_fields` and
    `_driving_keys_by_table` methods are mocked to return their expected values.
    """

    with mock.patch.object(
        SnowflakeDeserializer, "_fields", new_callable=PropertyMock, return_value=fields
    ):
        with mock.patch.object(
            SnowflakeDeserializer,
            "_driving_keys_by_table",
            new_callable=PropertyMock,
            return_value=driving_keys_by_table,
        ):
            # `Hub` deserialization.
            hub = snowflake_deserializer._deserialize_table("h_customer")
            compare_tables(hub, h_customer)

            # `RolePlayingHub` deserialization.
            role_playing_hub = snowflake_deserializer._deserialize_table(
                "h_customer_role_playing"
            )
            compare_tables(role_playing_hub, h_customer_role_playing)

            # `Link` deserialization.
            link = snowflake_deserializer._deserialize_table("l_order_customer")
            compare_tables(link, l_order_customer)

            # `Satellite` deserialization.
            satellite = snowflake_deserializer._deserialize_table("hs_customer")
            compare_tables(satellite, hs_customer)

            # `EffectivitySatellite` deserialization.
            effectivity_satellite = snowflake_deserializer._deserialize_table(
                "ls_order_customer_eff"
            )
            compare_tables(effectivity_satellite, ls_order_customer_eff)


def test_driving_keys_by_table(
    snowflake_deserializer: SnowflakeDeserializer,
    driving_keys_by_table: Dict[str, List[DrivingKeyField]],
):
    """Test `SnowflakeDeserializer._driving_keys_by_table` property."""
    for (
        table_name,
        driving_keys,
    ) in snowflake_deserializer._driving_keys_by_table.items():
        expected_driving_keys = driving_keys_by_table[table_name]

        assert len(driving_keys) == len(expected_driving_keys)

        for test_driving_key, expected_driving_key in zip(
            driving_keys, expected_driving_keys
        ):
            assert (
                test_driving_key.parent_table_name
                == expected_driving_key.parent_table_name
            )
            assert test_driving_key.name == expected_driving_key.name
            assert (
                test_driving_key.satellite_name == expected_driving_key.satellite_name
            )


def test_fields(
    snowflake_deserializer: SnowflakeDeserializer,
    fields_metadata: List[Dict[str, str]],
    fields_metadata_sql: str,
    target_tables: List[str],
    fields: Dict[str, List[Field]],
):
    """Test `SnowflakeDeserializer._fields` property.

    Given that this method accesses Snowflake to fetch the metadata of the target model,
    the `SnowflakeCursor` object is mocked and its results manipulated to match the
    result returned by Snowflake `SHOW COLUMNS` command.
    """
    # Mock `SnowflakeCursor` object and manipulate its results to match the model
    # metadata stored in `model_metadata.json`.
    cursor = snowflake_deserializer.database_connection.cursor
    cursor.return_value = MagicMock(SnowflakeCursor)
    cursor.return_value.__enter__().__iter__.return_value = iter(fields_metadata)
    calculated_fields = snowflake_deserializer._fields

    # Check if metadata query was called.
    cursor.return_value.__enter__().execute.assert_called_once_with(fields_metadata_sql)

    # Check that all tables have fields.
    assert len(calculated_fields.keys()) == len(target_tables)

    for table_name, table_fields in calculated_fields.items():
        # Check if the fields are the same as the expected result for the table.
        assert table_fields == fields[table_name]


def test_get_table_type(snowflake_deserializer: SnowflakeDeserializer):
    """Test `SnowflakeDeserializer._get_table_type` method."""
    # Check that all table types are properly calculated.
    assert snowflake_deserializer._get_table_type("h_customer") == Hub
    assert snowflake_deserializer._get_table_type("l_order_customer") == Link
    assert snowflake_deserializer._get_table_type("hs_customer") == Satellite
    assert (
        snowflake_deserializer._get_table_type("ls_order_customer_eff")
        == EffectivitySatellite
    )
    assert (
        snowflake_deserializer._get_table_type("h_customer_role_playing")
        == RolePlayingHub
    )

    # Check that a `RuntimeError` is raised when an invalid table name is provided.
    with pytest.raises(RuntimeError):
        snowflake_deserializer._get_table_type("invalid_table_name")


def test_deserialized_target_tables(
    snowflake_deserializer: SnowflakeDeserializer,
    target_tables: List[str],
    h_customer: Hub,
    fields: List[Field],
):
    """Test `SnowflakeDeserializer.deserialized_target_tables` property.

    In order to isolate the behaviour of this function,
    `SnowflakeDeserializer._fields` is mocked to ensure every table is
    deserialized correctly.
    """
    with mock.patch.object(
        SnowflakeDeserializer,
        "_fields",
        new_callable=PropertyMock,
        return_value=fields,
    ):
        deserialized_target_tables = snowflake_deserializer.deserialized_target_tables
        # Check that the number of deserialized tables is the same as the target tables
        # provided upon deserializer instantiation.
        assert len(deserialized_target_tables) == len(target_tables)

        for table in deserialized_target_tables:
            # Check that table name exist in target tables.
            assert table.name in target_tables

            # Check that the parent table was correctly attributed to role playing hub.
            if table.name == "h_customer_role_playing" and isinstance(
                table, RolePlayingHub
            ):
                compare_tables(table.parent_table, h_customer)


def test_database_configuration_with_password_invalid_input():
    """Test `DatabaseConfiguration` - no password nor authenticator=externalbrowser."""
    with pytest.raises(ValueError):
        _ = DatabaseConfiguration(
            database="some_db",
            user="some_user",
            warehouse="some_warehouse",
            account="some_account",
        )
