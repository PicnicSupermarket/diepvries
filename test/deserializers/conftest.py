"""Pytest fixtures for the deserializers."""

import json
from pathlib import Path
from typing import Dict, Iterator, List
from unittest import mock
from unittest.mock import Mock

import pytest
from snowflake.connector import SnowflakeConnection

from diepvries.deserializers.snowflake_deserializer import (
    METADATA_SQL_FILE_PATH,
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

# Pytest fixtures that depend on other fixtures defined in the same scope will
# trigger Pylint (Redefined name from outer scope). While usually valid, this doesn't
# make much sense in this case.
# pylint: disable=redefined-outer-name


@pytest.fixture
def target_schema() -> str:
    """Get target schema name."""
    return "dv"


@pytest.fixture
def target_tables() -> List[str]:
    """Build list of tables to be deserialized."""
    return [
        "h_customer",
        "h_customer_role_playing",
        "h_order",
        "l_order_customer",
        "l_order_customer_role_playing",
        "hs_customer",
        "ls_order_customer_eff",
        "ls_order_customer_role_playing_eff",
    ]


@pytest.fixture
def database_configuration() -> DatabaseConfiguration:
    """Instantiate database configuration to be provided to the deserializer.

    Given that the connection to Snowflake is mocked, all attributes have dummy values.
    """
    return DatabaseConfiguration(
        database="some_db",
        user="some_user",
        password="some_password",
        warehouse="some_warehouse",
        account="some_account",
    )


@pytest.fixture
def driving_keys_by_table(
    ls_order_customer_eff_driving_keys: List[DrivingKeyField],
    ls_order_customer_role_playing_eff_driving_keys: List[DrivingKeyField],
) -> Dict[str, List[DrivingKeyField]]:
    """Build dictionary of `DrivingKeyField`s indexed by table name."""
    return {
        "ls_order_customer_eff": ls_order_customer_eff_driving_keys,
        "ls_order_customer_role_playing_eff": ls_order_customer_role_playing_eff_driving_keys,
    }


@pytest.fixture
def driving_keys(
    driving_keys_by_table: Dict[str, List[DrivingKeyField]]
) -> List[DrivingKeyField]:
    """Build list of `DrivingKeyField`s in the target model."""
    return [
        driving_key
        for driving_keys in driving_keys_by_table.values()
        for driving_key in driving_keys
    ]


@pytest.fixture
def fields(
    h_customer: Hub,
    h_customer_role_playing: RolePlayingHub,
    h_order: Hub,
    l_order_customer: Link,
    l_order_customer_role_playing: Link,
    hs_customer: Satellite,
    ls_order_customer_eff: EffectivitySatellite,
    ls_order_customer_role_playing_eff: EffectivitySatellite,
) -> Dict[str, List[Field]]:
    """Build expected result for `SnowflakeDeserializer._fields` property."""
    return {
        "h_customer": h_customer.fields,
        "h_customer_role_playing": h_customer_role_playing.fields,
        "h_order": h_order.fields,
        "l_order_customer": l_order_customer.fields,
        "l_order_customer_role_playing": l_order_customer_role_playing.fields,
        "hs_customer": hs_customer.fields,
        "ls_order_customer_eff": ls_order_customer_eff.fields,
        "ls_order_customer_role_playing_eff": ls_order_customer_role_playing_eff.fields,
    }


@pytest.fixture
def role_playing_hubs() -> Dict[str, str]:
    """Build role playing hubs definition."""
    return {"h_customer_role_playing": "h_customer"}


@pytest.fixture
def fields_metadata_sql(
    target_schema: str,
    database_configuration: DatabaseConfiguration,
) -> str:
    """Get SQL used to fetch the target model metadata from Snowflake."""
    target_database = database_configuration.database
    return METADATA_SQL_FILE_PATH.read_text().format(
        target_database=target_database, target_schema=target_schema
    )


@pytest.fixture
def fields_metadata() -> List[Dict[str, str]]:
    """Get expected results for Snowflake `SHOW COLUMNS` command."""
    metadata = json.loads((Path(__file__).parent / "model_metadata.json").read_text())
    return metadata


@pytest.fixture
def snowflake_deserializer(
    target_schema: str,
    target_tables: List[str],
    database_configuration: DatabaseConfiguration,
    driving_keys: List[DrivingKeyField],
    role_playing_hubs: Dict[str, str],
) -> Iterator[SnowflakeDeserializer]:
    """Instantiate `SnowflakeDeserializer` used in unit tests.

    Given that the connection to Snowflake is created during object instantiation, the
    `connect` method from the Snowflake connector is mocked.
    """
    with mock.patch(
        "diepvries.deserializers.snowflake_deserializer.connect",
        return_value=Mock(SnowflakeConnection),
    ):
        yield SnowflakeDeserializer(
            target_schema=target_schema,
            target_tables=target_tables,
            database_configuration=database_configuration,
            driving_keys=driving_keys,
            role_playing_hubs=role_playing_hubs,
        )
