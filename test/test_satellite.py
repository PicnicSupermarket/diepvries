"""Unit test for Satellite."""

from pathlib import Path

from diepvries import FieldRole
from diepvries.data_vault_load import DataVaultLoad
from diepvries.satellite import Satellite


def test_effectivity_satellite_sql(test_path: Path, data_vault_load: DataVaultLoad):
    """Assert correctness of SQL generated in Effectivity Satellite class.

     (for ls_order_customer_eff - effectivity satellite)

    This test has to receive data_vault_load fixture and not ls_order_customer_eff
    as it needs the parent_table defined and this definition occurs in data_vault_load
    constructor.

    Args:
        test_path: Test path fixture value.
        data_vault_load: Data vault load fixture value.
    """
    effectivity_satellite = next(
        filter(
            lambda x: x.name == "ls_order_customer_eff", data_vault_load.target_tables
        )
    )

    expected_result = (
        test_path / "sql" / "expected_result_effectivity_satellite.sql"
    ).read_text()
    assert effectivity_satellite.sql_load_statement == expected_result


def test_satellite_load_sql(test_path: Path, hs_customer: Satellite):
    """Assert correctness of SQL generated in Satellite class.
    Args:
        test_path: Test path fixture value.
        hs_customer: Satellite fixture value.
    """
    expected_result = (test_path / "sql" / "expected_result_satellite.sql").read_text()
    assert hs_customer.sql_load_statement == expected_result


def test_set_field_roles(hs_customer: Satellite):
    """Assert correctness of field_roles attributed to hs_customer fields.

    Args:
        hs_customer: hs_customer fixture value.
    """
    expected_roles = [
        {"field": "h_customer_hashkey", "role": FieldRole.HASHKEY_PARENT},
        {"field": "r_timestamp", "role": FieldRole.METADATA},
        {"field": "r_timestamp_end", "role": FieldRole.METADATA},
        {"field": "r_source", "role": FieldRole.METADATA},
        {"field": "s_hashdiff", "role": FieldRole.HASHDIFF},
        {"field": "test_date", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_string", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_timestamp_ntz", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_integer", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_decimal", "role": FieldRole.DESCRIPTIVE},
        {"field": "x_customer_id", "role": FieldRole.DESCRIPTIVE},
        {"field": "grouping_key", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_geography", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_array", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_object", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_variant", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_timestamp_tz", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_timestamp_ltz", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_time", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_boolean", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_real", "role": FieldRole.DESCRIPTIVE},
    ]

    for field in hs_customer.fields:
        expected_role = next(
            role["role"] for role in expected_roles if role["field"] == field.name
        )
        assert field.role == expected_role


def test_parent_table_name(hs_customer: Satellite):
    """Assert correctness of parent table name.

    Args:
        hs_customer: hs_customer fixture value.
    """
    assert hs_customer.parent_table_name == "h_customer"


def test_hashdiff_sql(test_path: Path, data_vault_load: DataVaultLoad):
    """Assert correctness of SQL generated in Satellite class.

     (for hs_customer hashdiff)

    This test has to receive data_vault_load fixture and not hs_customer as it needs the
    parent_table defined and this definition occurs in data_vault_load constructor.

    Args:
        data_vault_load: Data vault load fixture value.
    """
    satellite = next(
        filter(lambda x: x.name == "hs_customer", data_vault_load.target_tables)
    )
    assert isinstance(satellite, Satellite)

    expected_result = (test_path / "sql" / "expected_result_hashdiff.sql").read_text()
    assert satellite.hashdiff_sql == expected_result.rstrip("\n")
