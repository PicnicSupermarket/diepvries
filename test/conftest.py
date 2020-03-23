import re
from datetime import datetime, timezone
from pathlib import Path

import pytest
import yaml
from picnic.data_vault import FieldDataType
from picnic.data_vault.data_vault_field import DataVaultField
from picnic.data_vault.data_vault_load import DataVaultLoad
from picnic.data_vault.driving_key_field import DrivingKeyField
from picnic.data_vault.effectivity_satellite import EffectivitySatellite
from picnic.data_vault.hub import Hub
from picnic.data_vault.link import Link
from picnic.data_vault.role_playing_hub import RolePlayingHub
from picnic.data_vault.satellite import Satellite


# Regex used to remove comments in SQL queries.
COMMENT_REGEX = re.compile(r"/\*{1}[^\/\*]+\*\/")


@pytest.fixture
def extract_start_timestamp():
    """
    Define extraction start timestamp for full test suite.

    Returns:
        datetime.datetime: extraction start timestamp used for testing.
    """
    timestamp = datetime(2019, 8, 6, tzinfo=timezone.utc)
    return timestamp


@pytest.fixture
def staging_table():
    """
    Define staging table physical name for full test suite.

    Returns:
        str: staging table physical name.
    """
    return "orders_20190806_000000"


@pytest.fixture
def test_path():
    """
    Define test path for full test suite.

    Returns:
        Path: current directory.
    """
    return Path(__file__).resolve().parent


@pytest.fixture
def process_configuration(test_path):
    """
    Make process configuration yml available for full test suite.

    Args:
        test_path (Path): test path fixture value.

    Returns:
        List[Dict[str, str]]: configuration yaml file content.
    """
    process_configuration_text = (test_path / "test_process_config.yml").read_text()
    process_configuration_file = yaml.safe_load(process_configuration_text)

    return process_configuration_file


@pytest.fixture
def h_customer(process_configuration, staging_table):
    """
    Define h_customer test hub for full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        Hub: deserialized h_customer.
    """
    h_customer_fields = [
        DataVaultField(
            parent_table_name="h_customer",
            name="h_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="h_customer",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=2,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="h_customer",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
        ),
        DataVaultField(
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
    return h_customer


@pytest.fixture
def h_customer_test_role_playing(process_configuration, h_customer, staging_table):
    """
    Define h_customer_test_role_playing test hub for full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        RolePlayingHub: deserialized h_customer_test_role_playing.
    """
    h_customer_test_role_playing_fields = [
        DataVaultField(
            parent_table_name="h_customer_test_role_playing",
            name="h_customer_test_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="h_customer_test_role_playing",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=2,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="h_customer_test_role_playing",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="h_customer_test_role_playing",
            name="customer_test_role_playing_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
    ]

    h_customer_test_role_playing = RolePlayingHub(
        schema=process_configuration["target_schema"],
        name="h_customer_test_role_playing",
        fields=h_customer_test_role_playing_fields,
    )
    h_customer_test_role_playing.parent_table = h_customer

    return h_customer_test_role_playing


@pytest.fixture
def h_order(process_configuration, staging_table):
    """
    Define h_order test hub for full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        Hub: deserialized h_order.
    """
    h_order_fields = [
        DataVaultField(
            parent_table_name="h_order",
            name="h_order_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="h_order",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=2,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="h_order",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
        ),
        DataVaultField(
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
    return h_order


@pytest.fixture
def l_order_customer(process_configuration, staging_table):
    """
    Define l_order_customer test link for full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        Link: deserialized l_order_customer.
    """
    l_order_customer_fields = [
        DataVaultField(
            parent_table_name="l_order_customer",
            name="l_order_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="l_order_customer",
            name="h_order_hashkey",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="l_order_customer",
            name="h_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="l_order_customer",
            name="order_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer",
            name="customer_id",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer",
            name="ck_test_string",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer",
            name="ck_test_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=7,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=8,
            is_mandatory=True,
        ),
        DataVaultField(
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
    return l_order_customer


@pytest.fixture
def l_order_customer_test_role_playing(process_configuration, staging_table):
    """
    Define l_order_customer_test_role_playing test link for full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        Link: deserialized l_order_customer_test_role_playing.
    """
    l_order_customer_test_role_playing_fields = [
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="l_order_customer_test_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="h_order_hashkey",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="h_customer_test_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=3,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="order_id",
            data_type=FieldDataType.TEXT,
            position=4,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="customer_test_role_playing_id",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="ck_test_string",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="ck_test_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=7,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=8,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="l_order_customer_test_role_playing",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=9,
            is_mandatory=True,
        ),
    ]
    l_order_customer_test_role_playing = Link(
        schema=process_configuration["target_schema"],
        name="l_order_customer_test_role_playing",
        fields=l_order_customer_test_role_playing_fields,
    )
    return l_order_customer_test_role_playing


@pytest.fixture
def hs_customer(process_configuration, staging_table):
    """
    Define hs_customer test satellite for full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        Satellite: deserialized hs_customer.
    """
    hs_customer_fields = [
        DataVaultField(
            parent_table_name="hs_customer",
            name="h_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="s_hashdiff",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=3,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="r_timestamp_end",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=4,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="test_string",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=False,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="test_date",
            data_type=FieldDataType.DATE,
            position=7,
            is_mandatory=False,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="test_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=8,
            is_mandatory=False,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="test_integer",
            data_type=FieldDataType.NUMBER,
            position=9,
            is_mandatory=False,
            precision=38,
            scale=0,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="test_decimal",
            data_type=FieldDataType.NUMBER,
            position=10,
            is_mandatory=False,
            precision=18,
            scale=8,
        ),
        DataVaultField(
            parent_table_name="hs_customer",
            name="x_customer_id",
            data_type=FieldDataType.TEXT,
            position=11,
            is_mandatory=False,
        ),
    ]
    hs_customer = Satellite(
        schema=process_configuration["target_schema"],
        name="hs_customer",
        fields=hs_customer_fields,
    )
    return hs_customer


@pytest.fixture
def ls_order_customer_eff(process_configuration, staging_table, l_order_customer):
    """
    Define ls_order_customer_eff test (effectivity) satellite for full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        Satellite: deserialized ls_order_customer_eff.
    """
    ls_order_customer_eff_fields = [
        DataVaultField(
            parent_table_name="ls_order_customer_eff",
            name="l_order_customer_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_eff",
            name="s_hashdiff",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_eff",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=3,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_eff",
            name="r_timestamp_end",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=4,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_eff",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_eff",
            name="dummy_descriptive_field",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
    ]

    driving_keys = [
        DrivingKeyField(
            name="h_customer_hashkey",
            parent_table_name="l_order_customer",
            satellite_name="ls_order_customer_eff",
        )
    ]

    ls_order_customer_eff = EffectivitySatellite(
        schema=process_configuration["target_schema"],
        name="ls_order_customer_eff",
        fields=ls_order_customer_eff_fields,
        driving_keys=driving_keys,
    )
    return ls_order_customer_eff


@pytest.fixture
def ls_order_customer_test_role_playing_eff(
    process_configuration, staging_table, l_order_customer_test_role_playing
):
    """
    Define ls_order_customer_test_role_playing_eff test (effectivity) satellite for
    full test suite.

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        staging_table (str): staging table physical name fixture value.

    Returns:
        Satellite: deserialized ls_order_customer_test_role_playing_eff.
    """
    ls_order_customer_test_role_playing_eff_fields = [
        DataVaultField(
            parent_table_name="ls_order_customer_test_role_playing_eff",
            name="l_order_customer_test_role_playing_hashkey",
            data_type=FieldDataType.TEXT,
            position=1,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_test_role_playing_eff",
            name="s_hashdiff",
            data_type=FieldDataType.TEXT,
            position=2,
            is_mandatory=True,
            length=32,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_test_role_playing_eff",
            name="r_timestamp",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=3,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_test_role_playing_eff",
            name="r_timestamp_end",
            data_type=FieldDataType.TIMESTAMP_NTZ,
            position=4,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_test_role_playing_eff",
            name="r_source",
            data_type=FieldDataType.TEXT,
            position=5,
            is_mandatory=True,
        ),
        DataVaultField(
            parent_table_name="ls_order_customer_test_role_playing_eff",
            name="dummy_descriptive_field",
            data_type=FieldDataType.TEXT,
            position=6,
            is_mandatory=True,
        ),
    ]

    driving_keys = [
        DrivingKeyField(
            name="h_customer_test_role_playing_hashkey",
            parent_table_name="l_order_customer_test_role_playing",
            satellite_name="ls_order_customer_test_role_playing_eff",
        )
    ]

    ls_order_customer_test_role_playing_eff = EffectivitySatellite(
        schema=process_configuration["target_schema"],
        name="ls_order_customer_test_role_playing_eff",
        fields=ls_order_customer_test_role_playing_eff_fields,
        driving_keys=driving_keys,
    )
    return ls_order_customer_test_role_playing_eff


@pytest.fixture
def data_vault_load(
    process_configuration,
    extract_start_timestamp,
    h_customer,
    h_order,
    l_order_customer,
    hs_customer,
    ls_order_customer_eff,
):
    """
    Create an instance of DataVaultLoad including all test tables
    (using fixture values defined above).

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        extract_start_timestamp (datetime.datetime): extraction start timestamp
            fixture value.
        h_customer (Hub): deserialized h_customer.
        h_order (Hub): deserialized h_order.
        l_order_customer (Link): deserialized l_order_customer.
        hs_customer (Satellite): deserialized hs_customer.
        ls_order_customer_eff (Satellite): deserialized ls_order_customer_eff.

    Returns:
        DataVaultLoad: test DataVaultLoad.
    """
    target_tables = [
        h_customer,
        h_order,
        l_order_customer,
        hs_customer,
        ls_order_customer_eff,
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


@pytest.fixture
def data_vault_load_with_role_playing(
    process_configuration,
    extract_start_timestamp,
    h_customer_test_role_playing,
    h_order,
    l_order_customer_test_role_playing,
    ls_order_customer_test_role_playing_eff,
):
    """
    Create an instance of DataVaultLoad for a model with a role playing hub
    (using fixture values defined above).

    Args:
        process_configuration (List[Dict[str, str]]): process configuration fixture
            value.
        extract_start_timestamp (datetime.datetime): extraction start timestamp
            fixture value.
        h_customer_test_role_playing (Hub): deserialized h_customer.
        h_order (Hub): deserialized h_order.
        l_order_customer_test_role_playing (Link): deserialized l_order_customer.
        ls_order_customer_test_role_playing_eff (Satellite): deserialized
            ls_order_customer_eff.

    Returns:
        DataVaultLoad: test DataVaultLoad with role playing hub.
    """
    target_tables = [
        h_customer_test_role_playing,
        h_order,
        l_order_customer_test_role_playing,
        ls_order_customer_test_role_playing_eff,
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


def clean_sql(sql: str) -> str:
    """
    Removes extra spaces and comments (in /**/ format) from a string,
    for SQL comparison purposes.

    Args:
        sql (str): SQL command to be cleaned.

    Returns:
        str: clean SQL command.
    """
    _sql = COMMENT_REGEX.sub("", sql)
    _sql = " ".join(_sql.split()).replace(";", "")

    return _sql
