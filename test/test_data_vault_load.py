"""Unit tests for Data Vault load."""

from pathlib import Path

from diepvries.data_vault_load import DataVaultLoad
from diepvries.effectivity_satellite import EffectivitySatellite
from diepvries.hub import Hub
from diepvries.link import Link
from diepvries.role_playing_hub import RolePlayingHub
from diepvries.satellite import Satellite


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


def test_data_vault_load_sql_by_group(
    test_path: Path,
    data_vault_load: DataVaultLoad,
    h_customer: Hub,
    h_customer_role_playing: RolePlayingHub,
    h_order: Hub,
    l_order_customer: Link,
    l_order_customer_role_playing: Link,
    hs_customer: Satellite,
    ls_order_customer_eff: EffectivitySatellite,
    ls_order_customer_role_playing_eff: EffectivitySatellite,
):
    """Assert correctness of a full DataVault load grouped by execution order.

    Args:
        test_path: Test path fixture value.
        data_vault_load: Data vault load fixture value.
    """
    groups = data_vault_load.sql_load_scripts_by_group

    assert len(groups[0]) == 1  # staging table
    assert groups[0][0] == data_vault_load.staging_create_sql_statement

    assert len(groups[1]) == 3  # hubs
    assert groups[1][0] == h_customer.sql_load_statement
    assert groups[1][1] == h_customer_role_playing.sql_load_statement
    assert groups[1][2] == h_order.sql_load_statement

    assert len(groups[2]) == 2  # links
    assert groups[2][0] == l_order_customer.sql_load_statement
    assert groups[2][1] == l_order_customer_role_playing.sql_load_statement

    assert len(groups[3]) == 3  # satellites
    assert groups[3][0] == hs_customer.sql_load_statement
    assert groups[3][1] == ls_order_customer_eff.sql_load_statement
    assert groups[3][2] == ls_order_customer_role_playing_eff.sql_load_statement
