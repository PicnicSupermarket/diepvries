"""Unit tests for Data Vault load."""

from pathlib import Path

from diepvries.data_vault_load import DataVaultLoad


def test_staging_table_sql(test_path: Path, data_vault_load: DataVaultLoad):
    """Assert correctness of staging table creation SQL.

    Args:
        test_path: Test path fixture value.
        data_vault_load: Data vault load fixture value.
    """
    expected_result = (test_path / "sql" / "expected_result_staging.sql").read_text()
    assert data_vault_load.staging_create_sql_statement == expected_result


def test_data_vault_load_sql(test_path: Path, data_vault_load: DataVaultLoad):
    """Assert correctness of a full DataVault load script.

    Args:
        test_path: Test path fixture value.
        data_vault_load: Data vault load fixture value.
    """
    expected_result = (
        test_path / "sql" / "expected_result_data_vault_load.sql"
    ).read_text()
    assert "\n".join(data_vault_load.sql_load_script) == expected_result


def test_data_vault_load_sql_by_group(test_path: Path, data_vault_load: DataVaultLoad):
    """Assert correctness of a full DataVault load grouped by execution order.

    Args:
        test_path: Test path fixture value.
        data_vault_load: Data vault load fixture value.
    """
    groups = data_vault_load.sql_load_scripts_by_group

    assert len(groups[0]) == 1  # staging table
    assert groups[0][0].startswith("CREATE OR REPLACE TABLE dv_stg.orders_")

    assert len(groups[1]) == 3  # hubs
    assert groups[1][0].startswith("MERGE INTO dv.h_customer")
    assert groups[1][1].startswith("MERGE INTO dv.h_customer")
    assert groups[1][2].startswith("MERGE INTO dv.h_order")

    assert len(groups[2]) == 2  # links
    assert groups[2][0].startswith("MERGE INTO dv.l_order_customer")
    assert groups[2][1].startswith("MERGE INTO dv.l_order_customer_role_playing")

    assert len(groups[3]) == 3  # satellites
    assert groups[3][0].startswith("MERGE INTO dv.hs_customer")
    assert groups[3][1].startswith("MERGE INTO dv.ls_order_customer")
    assert groups[3][2].startswith("MERGE INTO dv.ls_order_customer_role_playing_eff")
