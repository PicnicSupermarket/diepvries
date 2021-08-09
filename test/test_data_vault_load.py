"""Unit tests for Data Vault load."""

from pathlib import Path

from diepvries.data_vault_load import DataVaultLoad


def test_staging_table_sql(test_path: Path, data_vault_load: DataVaultLoad):
    """Assert correctness of staging table creation SQL.

    Args:
        test_path: Test path fixture value.
        data_vault_load: Data vault load fixture value.
    """
    expected_results = (test_path / "sql" / "expected_results_staging.sql").read_text()
    assert expected_results == data_vault_load.staging_create_sql_statement


def test_data_vault_load_sql(test_path: Path, data_vault_load: DataVaultLoad):
    """Assert correctness of a full DataVault load script.

    Args:
        test_path: Test path fixture value.
        data_vault_load: Data vault load fixture value.
    """
    expected_results = (
        test_path / "sql" / "expected_results_data_vault_load.sql"
    ).read_text()
    assert expected_results == "\n".join(data_vault_load.sql_load_script)
