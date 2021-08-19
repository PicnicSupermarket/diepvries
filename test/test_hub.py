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
        assert field.role == expected_role


def test_hashkey_sql(h_order: Hub):
    """Assert correctness of hashkey in Hub class.

    Args:
        h_order: h_order fixture value.
    """
    expected_result = (
        "MD5(COALESCE(CAST(order_id AS TEXT), 'dv_unknown')) AS h_order_hashkey"
    )
    assert h_order.hashkey_sql == expected_result


def test_hub_load_sql(test_path: Path, h_customer: Hub):
    """Assert correctness of SQL generated in Hub class.

    Args:
        test_path: Test path fixture value.
        h_customer: h_customer fixture value.
    """
    expected_result = (test_path / "sql" / "expected_result_hub.sql").read_text()
    assert h_customer.sql_load_statement == expected_result


def test_role_playing_hub_load_sql(
    test_path: Path, h_customer_role_playing: RolePlayingHub
):
    """Assert correctness of SQL generated in role playing Hub class.

    Args:
        test_path: Test path fixture value.
        h_customer_role_playing: Role playing hub fixture value.
    """
    expected_result = (
        test_path / "sql" / "expected_result_role_playing_hub.sql"
    ).read_text()
    assert h_customer_role_playing.sql_load_statement == expected_result
