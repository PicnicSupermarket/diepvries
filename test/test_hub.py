"""Unit tests for Hub."""

from pathlib import Path

from diepvries import FieldRole
from diepvries.hub import Hub
from diepvries.role_playing_hub import RolePlayingHub


def test_set_field_roles(h_order: Hub):
    """Assert correctness of field_roles attributed to h_order fields.

    Args:
        h_order: h_order fixture value.
    """
    expected_roles = [
        {"field": "order_id", "role": FieldRole.BUSINESS_KEY},
        {"field": "r_timestamp", "role": FieldRole.METADATA},
        {"field": "h_order_hashkey", "role": FieldRole.HASHKEY},
        {"field": "r_source", "role": FieldRole.METADATA},
    ]

    for field in h_order.fields:
        expected_role = next(
            role["role"] for role in expected_roles if role["field"] == field.name
        )
        assert expected_role == field.role


def test_hashkey_sql(h_order: Hub):
    """Assert correctness of hashkey in Hub class.

    Args:
        h_order: h_order fixture value.
    """
    expected_sql = """MD5(COALESCE(order_id, 'dv_unknown')) AS h_order_hashkey"""
    assert expected_sql == h_order.hashkey_sql


def test_hub_load_sql(test_path: Path, h_customer: Hub):
    """Assert correctness of SQL generated in Hub class.

    Args:
        test_path: Test path fixture value.
        h_customer: h_customer fixture value.
    """
    expected_results = (test_path / "sql" / "expected_results_hub.sql").read_text()
    assert expected_results == h_customer.sql_load_statement


def role_playing_hub_load_sql(test_path: Path, h_customer_role_playing: RolePlayingHub):
    """Assert correctness of SQL generated in role playing Hub class.

    Args:
        test_path: Test path fixture value.
        h_customer_role_playing: Role playing hub fixture value.
    """
    expected_results = (
        test_path / "sql" / "expected_results_role_playing_hub.sql"
    ).read_text()
    assert expected_results == h_customer_role_playing.sql_load_statement
