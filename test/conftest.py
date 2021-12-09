"""Pytest fixtures."""

from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pytest

from diepvries import FieldDataType
from diepvries.data_vault_load import DataVaultLoad
from diepvries.driving_key_field import DrivingKeyField
from diepvries.effectivity_satellite import EffectivitySatellite
from diepvries.field import Field
from diepvries.hub import Hub
from diepvries.link import Link
from diepvries.role_playing_hub import RolePlayingHub
from diepvries.satellite import Satellite
from diepvries.table import StagingTable

# Pytest fixtures that depend on other fixtures defined in the same scope will
# trigger Pylint (Redefined name from outer scope). While usually valid, this doesn't
# make much sense in this case.
# pylint: disable=redefined-outer-name


@pytest.fixture
def extract_start_timestamp() -> datetime:
    """Define extraction start timestamp.

    Returns:
        Extraction start timestamp used for testing.
    """
    timestamp = datetime(2019, 8, 6, tzinfo=timezone.utc)
    return timestamp


@pytest.fixture
def test_path() -> Path:
    """Define test path.

    Returns:
        Parent directory of this file.
    """
    return Path(__file__).resolve().parent


@pytest.fixture
def process_configuration() -> Dict[str, str]:
    """Define process configuration.

    Returns:
        Process configuration.
    """
    config = {
        "source": "test",
        "extract_schema": "dv_extract",
        "extract_table": "extract_orders",
        "staging_schema": "dv_stg",
        "staging_table": "orders",
        "target_schema": "dv",
    }
    return config


@pytest.fixture
def staging_table(process_configuration, extract_start_timestamp) -> StagingTable:
    """Return the staging table"""
    staging_table = StagingTable(
        schema=process_configuration["staging_schema"],
        name=process_configuration["staging_table"],
        extract_start_timestamp=extract_start_timestamp,
    )
    return staging_table


@pytest.fixture
def ls_order_customer_eff_driving_keys() -> List[DrivingKeyField]:
    """Build dictionary of driving keys, indexed by the satellite name."""
    return [
        DrivingKeyField(
            name="h_customer_hashkey",
            parent_table_name="l_order_customer",
            satellite_name="ls_order_customer_eff",
        )
    ]


@pytest.fixture
def ls_order_customer_role_playing_eff_driving_keys() -> List[DrivingKeyField]:
    """Build dictionary of driving keys, indexed by the satellite name."""
    return [
        DrivingKeyField(
            name="h_customer_role_playing_hashkey",
            parent_table_name="l_order_customer_role_playing",
            satellite_name="ls_order_customer_role_playing_eff",
        )
    ]


@pytest.fixture
def h_customer(
    process_configuration: Dict[str, str], staging_table: StagingTable
) -> Hub:
    """Define h_customer test hub.

    Args:
        process_configuration: Process configuration fixture value.
        staging_table: Staging table fixture value.

    Returns:
        Deserialized hub h_customer.
    """
    h_customer_fields = [
        Field(
            parent_table_name="h_customer",
            name="h_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="h_customer",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=2,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="h_customer",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="h_customer",
            name="customer_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
    ]

    h_customer = Hub(
        schema=process_configuration["target_schema"],
        name="h_customer",
        fields=h_customer_fields,
    )
    h_customer.staging_table = staging_table

    return h_customer


@pytest.fixture
def h_customer_role_playing(
    process_configuration: Dict[str, str], h_customer: Hub, staging_table: StagingTable
) -> RolePlayingHub:
    """Define h_customer_role_playing test hub.

    Args:
        process_configuration: Process configuration fixture value.
        h_customer: Hub customer fixture value.
        staging_table: Staging table fixture value.

    Returns:
        Deserialized role playing hub h_customer_role_playing.
    """
    h_customer_role_playing_fields = [
        Field(
            parent_table_name="h_customer_role_playing",
            name="h_customer_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="h_customer_role_playing",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=2,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="h_customer_role_playing",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="h_customer_role_playing",
            name="customer_role_playing_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
    ]

    h_customer_role_playing = RolePlayingHub(
        schema=process_configuration["target_schema"],
        name="h_customer_role_playing",
        fields=h_customer_role_playing_fields,
    )
    h_customer_role_playing.parent_table = h_customer
    h_customer_role_playing.staging_table = staging_table

    return h_customer_role_playing


@pytest.fixture
def h_order(process_configuration: Dict[str, str], staging_table: StagingTable) -> Hub:
    """Define h_order test hub.

    Args:
        process_configuration: Process configuration fixture value.
        staging_table: StagingTable fixture value.

    Returns:
        Deserialized hub h_order.
    """
    h_order_fields = [
        Field(
            parent_table_name="h_order",
            name="h_order_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="h_order",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=2,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="h_order",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="h_order",
            name="order_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
    ]
    h_order = Hub(
        schema=process_configuration["target_schema"],
        name="h_order",
        fields=h_order_fields,
    )
    h_order.staging_table = staging_table
    return h_order


@pytest.fixture
def l_order_customer(
    process_configuration: Dict[str, str], staging_table: StagingTable
) -> Link:
    """Define l_order_customer test link.

    Args:
        process_configuration: Process configuration fixture value.
        staging_table: Staging table fixture value.

    Returns:
        Deserialized link l_order_customer.
    """
    l_order_customer_fields = [
        Field(
            parent_table_name="l_order_customer",
            name="l_order_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="h_order_hashkey",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="h_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="order_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="customer_id",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="ck_test_string",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="ck_test_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=7,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=8,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=9,
            is_mandatory=True,
        ),
    ]
    l_order_customer = Link(
        schema=process_configuration["target_schema"],
        name="l_order_customer",
        fields=l_order_customer_fields,
    )
    l_order_customer.staging_table = staging_table

    return l_order_customer


@pytest.fixture
def l_order_customer_role_playing(process_configuration: Dict[str, str]) -> Link:
    """Define l_order_customer_role_playing test link.

    Args:
        process_configuration: Process configuration fixture value.

    Returns:
        Deserialized link l_order_customer_role_playing.
    """
    l_order_customer_role_playing_fields = [
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="l_order_customer_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="h_order_hashkey",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="h_customer_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="order_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="customer_role_playing_id",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="ck_test_string",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="ck_test_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=7,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=8,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="l_order_customer_role_playing",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=9,
            is_mandatory=True,
        ),
    ]
    l_order_customer_role_playing = Link(
        schema=process_configuration["target_schema"],
        name="l_order_customer_role_playing",
        fields=l_order_customer_role_playing_fields,
    )
    return l_order_customer_role_playing


@pytest.fixture
def hs_customer(
    process_configuration: Dict[str, str], staging_table: StagingTable
) -> Satellite:
    """Define hs_customer test satellite.

    Args:
        process_configuration: Process configuration fixture value.
        staging_table: Staging table fixture value.

    Returns:
        Deserialized satellite hs_customer.
    """
    hs_customer_fields = [
        Field(
            parent_table_name="hs_customer",
            name="h_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="hs_customer",
            name="s_hashdiff",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="hs_customer",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=3,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="hs_customer",
            name="r_timestamp_end",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=4,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="hs_customer",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_string",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_date",
            data_type=FieldDataType.DATE,
            position=7,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_timestamp_ntz",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=8,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_integer",
            data_type=FieldDataType.NUMBER,
            position=9,
            is_mandatory=False,
            precision=38,
            scale=0,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_decimal",
            data_type=FieldDataType.NUMBER,
            position=10,
            is_mandatory=False,
            precision=18,
            scale=8,
        ),
        Field(
            parent_table_name="hs_customer",
            name="x_customer_id",
            data_type=FieldDataType.TEXT,
            position=11,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="grouping_key",
            data_type=FieldDataType.TEXT,
            position=12,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_geography",
            data_type=FieldDataType.GEOGRAPHY,
            position=13,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_array",
            data_type=FieldDataType.ARRAY,
            position=14,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_object",
            data_type=FieldDataType.OBJECT,
            position=15,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_variant",
            data_type=FieldDataType.VARIANT,
            position=16,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_timestamp_tz",
            data_type=FieldDataType.TIMESTAMP_TZ,
            position=17,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_timestamp_ltz",
            data_type=FieldDataType.TIMESTAMP_LTZ,
            position=18,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_time",
            data_type=FieldDataType.TIME,
            position=19,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_boolean",
            data_type=FieldDataType.BOOLEAN,
            position=20,
            is_mandatory=False,
        ),
        Field(
            parent_table_name="hs_customer",
            name="test_real",
            data_type=FieldDataType.REAL,
            position=21,
            is_mandatory=False,
        ),
    ]
    hs_customer = Satellite(
        schema=process_configuration["target_schema"],
        name="hs_customer",
        fields=hs_customer_fields,
    )
    hs_customer.staging_table = staging_table

    return hs_customer


@pytest.fixture
def ls_order_customer_eff(
    process_configuration: Dict[str, str],
    ls_order_customer_eff_driving_keys: List[DrivingKeyField],
) -> EffectivitySatellite:
    """Define ls_order_customer_eff test (effectivity) satellite.

    Args:
        process_configuration: Process configuration fixture value.
        ls_order_customer_eff_driving_keys: Driving key for satellite.

    Returns:
        Deserialized effectivity satellite ls_order_customer_eff.
    """
    ls_order_customer_eff_fields = [
        Field(
            parent_table_name="ls_order_customer_eff",
            name="l_order_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="ls_order_customer_eff",
            name="s_hashdiff",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="ls_order_customer_eff",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=3,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="ls_order_customer_eff",
            name="r_timestamp_end",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=4,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="ls_order_customer_eff",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="ls_order_customer_eff",
            name="dummy_descriptive_field",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
    ]

    ls_order_customer_eff = EffectivitySatellite(
        schema=process_configuration["target_schema"],
        name="ls_order_customer_eff",
        fields=ls_order_customer_eff_fields,
        driving_keys=ls_order_customer_eff_driving_keys,
    )
    return ls_order_customer_eff


@pytest.fixture
def ls_order_customer_role_playing_eff(
    process_configuration: Dict[str, str],
    ls_order_customer_role_playing_eff_driving_keys: List[DrivingKeyField],
) -> EffectivitySatellite:
    """Define ls_order_customer_role_playing_eff test (effectivity) satellite.

    Args:
        process_configuration: Process configuration fixture value.
        ls_order_customer_role_playing_eff_driving_keys: Driving key for satellite.

    Returns:
        Deserialized effectivity satellite ls_order_customer_role_playing_eff.
    """
    ls_order_customer_role_playing_eff_fields = [
        Field(
            parent_table_name="ls_order_customer_role_playing_eff",
            name="l_order_customer_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="ls_order_customer_role_playing_eff",
            name="s_hashdiff",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        Field(
            parent_table_name="ls_order_customer_role_playing_eff",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=3,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="ls_order_customer_role_playing_eff",
            name="r_timestamp_end",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=4,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="ls_order_customer_role_playing_eff",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        Field(
            parent_table_name="ls_order_customer_role_playing_eff",
            name="dummy_descriptive_field",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
    ]

    ls_order_customer_role_playing_eff = EffectivitySatellite(
        schema=process_configuration["target_schema"],
        name="ls_order_customer_role_playing_eff",
        fields=ls_order_customer_role_playing_eff_fields,
        driving_keys=ls_order_customer_role_playing_eff_driving_keys,
    )
    return ls_order_customer_role_playing_eff


@pytest.fixture
def data_vault_load(
    process_configuration: Dict[str, str],
    extract_start_timestamp: datetime,
    h_customer: Hub,
    h_customer_role_playing: RolePlayingHub,
    h_order: Hub,
    l_order_customer: Link,
    l_order_customer_role_playing: Link,
    hs_customer: Satellite,
    ls_order_customer_eff: EffectivitySatellite,
    ls_order_customer_role_playing_eff: EffectivitySatellite,
) -> DataVaultLoad:
    """Define an instance of DataVaultLoad that includes all test tables.

    Args:
        process_configuration: Process configuration fixture value.
        extract_start_timestamp: Extraction start timestamp fixture value.
        h_customer: Deserialized hub h_customer.
        h_customer_role_playing: Deserialized hub h_customer_role_playing.
        h_order: Deserialized hub h_order.
        l_order_customer: Deserialized link l_order_customer.
        l_order_customer_role_playing: Deserialized link l_order_customer_role_playing.
        hs_customer: Deserialized satellite hs_customer.
        ls_order_customer_eff: Deserialized effectivity satellite ls_order_customer_eff.
        ls_order_customer_role_playing_eff: Deserialized effectivity satellite
            ls_order_customer_role_playing_eff.

    Returns:
        Instance of DataVaultLoad suitable for testing.
    """
    target_tables = [
        h_customer,
        h_customer_role_playing,
        h_order,
        l_order_customer,
        l_order_customer_role_playing,
        hs_customer,
        ls_order_customer_eff,
        ls_order_customer_role_playing_eff,
    ]
    data_vault_load_configuration = {
        "extract_schema": process_configuration["extract_schema"],
        "extract_table": process_configuration["extract_table"],
        "staging_schema": process_configuration["staging_schema"],
        "staging_table": process_configuration["staging_table"],
        "target_tables": target_tables,
        "source": process_configuration["source"],
    }
    return DataVaultLoad(
        **data_vault_load_configuration, extract_start_timestamp=extract_start_timestamp
    )
