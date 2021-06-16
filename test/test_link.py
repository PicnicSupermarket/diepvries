"""Unit test for Link."""
from pathlib import Path

from diepvries import FieldRole
from diepvries.link import Link


def test_set_field_roles(l_order_customer: Link):
    """Assert correctness of field_roles attributed to l_order_customer fields.

    Args:
        l_order_customer: l_order_customer fixture value.
    """
    expected_roles = [
        {"field": "r_source", "role": FieldRole.METADATA},
        {"field": "r_timestamp", "role": FieldRole.METADATA},
        {"field": "order_id", "role": FieldRole.BUSINESS_KEY},
        {"field": "customer_id", "role": FieldRole.BUSINESS_KEY},
        {"field": "h_customer_hashkey", "role": FieldRole.HASHKEY_PARENT},
        {"field": "ck_test_string", "role": FieldRole.CHILD_KEY},
        {"field": "ck_test_timestamp", "role": FieldRole.CHILD_KEY},
        {"field": "l_order_customer_hashkey", "role": FieldRole.HASHKEY},
        {"field": "h_order_hashkey", "role": FieldRole.HASHKEY_PARENT},
    ]

    for field in l_order_customer.fields:
        expected_role = next(
            role["role"] for role in expected_roles if role["field"] == field.name
        )
        assert expected_role == field.role


def test_hashkey_sql(l_order_customer: Link):
    """Assert correctness of SQL generated in Link class (for l_order_customer_hashkey).

    Args:
        l_order_customer: l_order_customer fixture value.
    """
    expected_sql = (
        "MD5(COALESCE(order_id, 'dv_unknown')||'|~~|'||COALESCE(customer_id, "
        "'dv_unknown')||'|~~|'||COALESCE(CAST(ck_test_string AS VARCHAR), '')||'|~~|'||"
        "COALESCE(CAST(ck_test_timestamp AS VARCHAR), '')) AS l_order_customer_hashkey"
    )
    assert expected_sql == l_order_customer.hashkey_sql


def test_parent_hub_names(l_order_customer: Link):
    """Assert correctness of the names of the parent hubs in Link class.

     (l_order_customer_hashkey).

    Args:
        l_order_customer: l_order_customer fixture value.
    """
    expected_parent_hub_names = ["h_order", "h_customer"]
    for parent_hub in l_order_customer.parent_hub_names:
        assert parent_hub in expected_parent_hub_names


def test_link_load_sql(test_path: Path, l_order_customer: Link):
    """Assert correctness of SQL generated in Link class.

    Args:
        test_path: Test path fixture value.
        l_order_customer: l_order_customer fixture value.
    """
    expected_results = (test_path / "sql" / "expected_results_link.sql").read_text()
    assert expected_results == l_order_customer.sql_load_statement
