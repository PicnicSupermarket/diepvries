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

    expected_results = (
        test_path / "sql" / "expected_results_effectivity_satellite.sql"
    ).read_text()

    assert expected_results == effectivity_satellite.sql_load_statement


def test_satellite_load_sql(test_path: Path, hs_customer: Satellite):
    """Assert correctness of SQL generated in Satellite class.
    Args:
        test_path: Test path fixture value.
        hs_customer: Satellite fixture value.
    """
    expected_results = (
        test_path / "sql" / "expected_results_satellite.sql"
    ).read_text()

    assert expected_results == hs_customer.sql_load_statement


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
        {"field": "test_timestamp", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_integer", "role": FieldRole.DESCRIPTIVE},
        {"field": "test_decimal", "role": FieldRole.DESCRIPTIVE},
        {"field": "x_customer_id", "role": FieldRole.DESCRIPTIVE},
        {"field": "grouping_key", "role": FieldRole.DESCRIPTIVE},
    ]

    for field in hs_customer.fields:
        expected_role = next(
            role["role"] for role in expected_roles if role["field"] == field.name
        )
        assert expected_role == field.role


def test_parent_table_name(hs_customer: Satellite):
    """Assert correctness of parent table name.

    Args:
        hs_customer: hs_customer fixture value.
    """
    assert hs_customer.parent_table_name == "h_customer"


def test_hashdiff_sql(data_vault_load: DataVaultLoad):
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

    expected_sql = (
        "MD5(REGEXP_REPLACE(COALESCE(customer_id, 'dv_unknown')||'|~~|'||"
        "COALESCE(CAST(test_string AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_date AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_timestamp AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_integer AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_decimal AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(x_customer_id AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(grouping_key AS VARCHAR), ''), "
        "'(\\\\|~~\\\\|)+$', '')) AS hs_customer_hashdiff"
    )

    assert expected_sql == satellite.hashdiff_sql
