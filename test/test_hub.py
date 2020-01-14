from picnic.data_vault import FieldRole

from .conftest import clean_sql


def test_set_field_roles(h_order):
    """
    Compare field_roles attributed to h_order fields with expected results.

    Args:
        h_order (Hub): h_order fixture value (defined in conftest.py).
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


def test_hashkey_sql(h_order):
    """
    Compares SQL generated in Hub class (for h_order_hashkey) with expected values.

    Args:
        h_order (Hub): h_order fixture value (defined in conftest.py).
    """
    expected_sql = """MD5(COALESCE(order_id, 'dv_unknown')) AS h_order_hashkey"""
    assert clean_sql(expected_sql) == clean_sql(h_order.hashkey_sql)
