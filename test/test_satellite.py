from picnic.data_vault import FieldRole

from .conftest import clean_sql


def test_effectivity_satellite_sql(test_path, data_vault_load):
    """
    Compares SQL generated in Satellite class (for ls_order_customer_eff -
    effectivity satellite) with the expected results.

    This test has to receive data_vault_load fixture and not ls_order_customer_eff
    as it needs the parent_table defined and this definition occurs in data_vault_load
    constructor.

    Args:
        test_path (Path): test_path fixture value (defined in conftest.py).
        data_vault_load (DataVaultLoad): data_vault_load fixture value (defined in
            conftest.py).
    """
    effectivity_satellite = next(
        filter(
            lambda x: x.name == "ls_order_customer_eff", data_vault_load.target_tables
        )
    )

    expected_results = (
        test_path / "sql" / "expected_results_effectivity_satellite.sql"
    ).read_text()

    assert clean_sql(expected_results) == clean_sql(
        effectivity_satellite.sql_load_statement
    )


def test_set_field_roles(hs_customer):
    """
    Compare field_roles attributed to hs_customer fields with expected results.

    Args:
        hs_customer (Satellite): hs_customer fixture value (defined in conftest.py).
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


def test_parent_table_name(hs_customer):
    """
    Check if parent_table_name is being correctly calculated.

    Args:
        hs_customer (Satellite): hs_customer fixture value (defined in conftest.py).
    """
    assert hs_customer.parent_table_name == "h_customer"


def test_hashdiff_sql(data_vault_load):
    """
    Compares SQL generated in Satellite class (for hs_customer hashdiff) with expected
    values.

    This test has to receive data_vault_load fixture and not hs_customer as it needs the
    parent_table defined and this definition occurs in data_vault_load constructor.

    Args:
        data_vault_load (DataVaultLoad): data_vault_load fixture value (defined in
            conftest.py).
    """
    satellite = next(
        filter(lambda x: x.name == "hs_customer", data_vault_load.target_tables)
    )
    expected_sql = (
        "MD5(REGEXP_REPLACE(COALESCE(customer_id, 'dv_unknown')||'|~~|'||"
        "COALESCE(CAST(test_string AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_date AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_timestamp AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_integer AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(test_decimal AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(x_customer_id AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(grouping_key AS VARCHAR), ''), "
        "'(\\\\|~~\\\\|){1,}$', '')) AS hs_customer_hashdiff"
    )

    assert clean_sql(expected_sql) == clean_sql(satellite.hashdiff_sql)
