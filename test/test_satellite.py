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
        "MD5(REGEXP_REPLACE("
        "COALESCE(customer_id, 'dv_unknown')"
        "||'|~~|'||COALESCE(test_string, '')"
        "||'|~~|'||COALESCE(TO_CHAR(CAST(test_date AS DATE), 'yyyy-mm-dd'), '')"
        "||'|~~|'||COALESCE(TO_CHAR(CAST(test_timestamp_ntz AS TIMESTAMP_NTZ), "
        "'yyyy-mm-dd hh24:mi:ss.ff9'), '')"
        "||'|~~|'||COALESCE(CAST(CAST(test_integer AS NUMBER (38, 0)) AS TEXT), '')"
        "||'|~~|'||COALESCE(CAST(CAST(test_decimal AS NUMBER (18, 8)) AS TEXT), '')"
        "||'|~~|'||COALESCE(x_customer_id, '')||'|~~|'||COALESCE(grouping_key, '')"
        "||'|~~|'||COALESCE(ST_ASTEXT(TO_GEOGRAPHY(test_geography)), '')"
        "||'|~~|'||COALESCE(CAST(CAST(test_array AS ARRAY) AS TEXT), '')"
        "||'|~~|'||COALESCE(CAST(CAST(test_object AS OBJECT) AS TEXT), '')"
        "||'|~~|'||COALESCE(CAST(CAST(test_variant AS VARIANT) AS TEXT), '')"
        "||'|~~|'||COALESCE(TO_CHAR(CAST(test_timestamp_tz AS TIMESTAMP_TZ), "
        "'yyyy-mm-dd hh24:mi:ss.ff9 tzhtzm'), '')"
        "||'|~~|'||COALESCE(TO_CHAR(CAST(test_timestamp_ltz AS TIMESTAMP_LTZ), "
        "'yyyy-mm-dd hh24:mi:ss.ff9 tzhtzm'), '')"
        "||'|~~|'||COALESCE(TO_CHAR(CAST(test_time AS TIME), 'hh24:mi:ss.ff9'), ''), "
        "'(\\\\|~~\\\\|)+$', '')) AS hs_customer_hashdiff"
    )

    assert expected_sql == satellite.hashdiff_sql
