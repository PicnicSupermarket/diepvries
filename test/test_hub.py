"""Unit tests for Hub."""

from picnic.data_vault import FieldRole
from picnic.data_vault.hub import Hub

from .conftest import clean_sql


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
    """Assert correctness of SQL generated in Hub class.

    Args:
        h_order: h_order fixture value.
    """
    expected_sql = """MD5(COALESCE(order_id, 'dv_unknown')) AS h_order_hashkey"""
    assert clean_sql(expected_sql) == clean_sql(h_order.hashkey_sql)
